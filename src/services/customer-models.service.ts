
import {
    BindingScope,
    config,
    ContextTags,
    injectable,
    Provider,
} from '@loopback/core';
import {
    Request,
} from "@loopback/rest";
import multer from 'multer';
import { FILE_UPLOAD_SERVICE } from '../keys';
import { FileUploadHandler } from '../types';
import path from "path";
import fs from "fs";
import { runPythonScript, UploadS3, downloadFileFromS3IfNotAvailable, downloadFilesFromS3 } from '.';
import { CustomerIndustryRepository, CustomerModelsRepository, CustomerRepository, FileHistoryRepository } from "../repositories";
import { sendEmailToAdmin, sendMailChimpEmail } from "../helper"
import { stateObj } from "../constant/generate_leads";
import { GenerateLeadsService } from './';
import { CustomerModels, FileHistory } from "../models";
import { generatePresignedS3Url } from "../services";
import { extractPythonResponse } from "../helper/utils";
import { industryTypes, industrTypesMetaData } from "../constant/industry_type";
import { parse } from "csv-parse";
import { getFilterSort } from '../helper/filter-sort';
const csvParser = require("csv-parser");
@injectable({
    scope: BindingScope.TRANSIENT,
    tags: { [ContextTags.KEY]: FILE_UPLOAD_SERVICE },
})
export class CustomerModelsService implements Provider<FileUploadHandler> {
    constructor(@config() private options: multer.Options = {}) {
        if (!this.options.storage) {
            // Default to in-memory storage
            this.options.storage = multer.memoryStorage();
        }
    }

    value(): FileUploadHandler {
        return multer(this.options).any();
    }

    static unLinkFileIfExists(filePath: string) {
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath)
        }
    }

    static async getAllModels(
        request: Request,
        email: string,
        customerRepository: any,
        customerModelsRepository: CustomerModelsRepository,
        modelType: string,
        idAdmin: boolean
    ) {
        const whereObj: any = {};

        if (!idAdmin) {
            const findbyemail = await customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email }
            });
            if (!findbyemail)
                return { result: "Email not found" };
            whereObj['email'] = email;
        }

        if (modelType)
            whereObj['type'] = modelType;

        return customerModelsRepository.find({
            fields: ['email', 'name', 'error', 'error_detail', 'updated_at', 'created_at', 'vendor_list_url', 'description', 'status', 'id', 'insights'],
            where: whereObj,
            order: ['updated_at DESC']
        });
    }

    static async getModelsPaginated(
        request: Request,
        email: string,
        customerRepository: any,
        customerModelsRepository: CustomerModelsRepository,
        modelType: string,
        idAdmin: boolean
    ) {
        let where: any = {}
        let order: string = 'created_at DESC'

        if (!idAdmin) {
            const findbyemail = await customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email }
            });
            if (!findbyemail)
                return { result: "Email not found" };
            where['email'] = email
        }


        //

        if (modelType)
            where['type'] = modelType;

        const searchEmail: any = request.query.email;
        const sortName: any = request.query.sortName;
        const type: any = request.query.type;
        const page: any = request.query.page || 1;

        if (searchEmail) {
            where['email'] = searchEmail
        }


        let filtertype = ""
        if (modelType === "lead_sorting") {
            filtertype = 'leadsorting model'
        } else {
            filtertype = 'leadgeneration model'
        }
        const filterSort = getFilterSort({ where, filtertype, sortName, type })

        where = filterSort.where
        if (filterSort.order !== '') {
            order = filterSort.order
        }

        const limit = 10;
        const offset = (page - 1) * limit;

        const totalCountPromise = await customerModelsRepository.count(where);

        let previous_files = await customerModelsRepository.find({
            fields: ['email', 'name', 'error', 'error_detail', 'updated_at', 'created_at', 'vendor_list_url', 'description', 'status', 'id', 'insights'],
            where: where, order: [order], limit: limit, skip: offset
        });

        const [totalCount, data] = await Promise.all([totalCountPromise, previous_files]);
        const length = totalCount?.count

        // return previous_files
        return {
            length: length,
            data: data
        };

        //

        // return customerModelsRepository.find({
        //     fields: ['email', 'name', 'error', 'error_detail', 'updated_at', 'created_at', 'vendor_list_url', 'description', 'status', 'id', 'insights'],
        //     where: whereObj,
        //     order: ['updated_at DESC']
        //   });
    }

    static async fileValidateService(customerRepository: any, customerModelsRepository: CustomerModelsRepository, customerIndustryRepository: CustomerIndustryRepository, email: string, uploadedFileName: string) {
        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');

        let file = path.join(__dirname, `../../.sandbox/${filename_without_extension}.csv`);
        // let outputFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}-columnmappingv2.csv`);
        const outputFileName = `${filename_without_extension}-columnmappingv2.csv`;
        let outputFilePath = path.join(__dirname, `../../.sandbox/${outputFileName}`);
        let responseToSend: any = {
            msg: "File started processing",
            statusCode: 200
        }
        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });

            const customerModelDataPromise = customerModelsRepository.findOne({
                where: { email: email, vendor_list_url: uploadedFileName, status: 2 },
            });
            const findIndustryPromise = customerIndustryRepository.findOne({
                where: { email },
            });
            const [customerModelData, findbyemail, customerIndustry] = await Promise.all([customerModelDataPromise, findbyemailPromise, findIndustryPromise]);
            if (customerModelData) {
                await downloadFileFromS3IfNotAvailable(file, email);
                let industryType = customerIndustry?.industry_type;
                const scriptPath = path.join(__dirname, '../../python_models/lead_generation/column_mappingv2.py');
                let args = ["--file_path", file];

                let industrialProfileQuestionnaire = customerModelData?.industry_profile || {};
                let industrialProfileToSend = { ...industrialProfileQuestionnaire, industry_type: industryType }
                args.push("--industry_profile");
                args.push(JSON.stringify(industrialProfileToSend));

                let python_output: any = await runPythonScript(scriptPath, args);
                const { output } = await extractPythonResponse({ python_output });
                if (output.success == 'True') {

                    customerModelData.status = 3;
                    await customerModelsRepository.update(customerModelData)
                    // upload output file to s3
                    await UploadS3(outputFileName, fs.createReadStream(outputFilePath), email);
                    CustomerModelsService.filePreProcessService(customerRepository, customerModelsRepository, customerIndustryRepository, email, uploadedFileName)
                }
                else {
                    customerModelData.error = "column_mappingv2";
                    customerModelData.error_detail = output.error_details;
                    const optionsforAdminMail = {
                        error: customerModelData.error,
                        errorDetails: customerModelData.error_detail,
                        content: null
                    }
                    // customer_model_obj = await this.customerModelsRepository.update(customer_model_obj)
                    await customerModelsRepository.update(customerModelData);
                    sendEmailToAdmin(uploadedFileName, findbyemail, customerRepository, optionsforAdminMail)
                    responseToSend.msg = customerModelData.error;
                    responseToSend.statusCode = 400;
                }
            }
            else {
                responseToSend.msg = 'ERROR - customer-models.service#fileValidateService, no model history found for file'
                responseToSend.statusCode = 400;
                console.error("responseToSend.msg", responseToSend.msg)
            }
        } catch (e) {
            responseToSend.msg = e.message;
            responseToSend.statusCode = 400;
        } finally {
            this.unLinkFileIfExists(outputFilePath)
            this.unLinkFileIfExists(file)
        }
        return responseToSend

    }

    static async filePreProcessService(customerRepository: any, customerModelsRepository: CustomerModelsRepository, 
        customerIndustryRepository: CustomerIndustryRepository, email: string, uploadedFileName: string) {
        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let file = path.join(__dirname, `../../.sandbox/${filename_without_extension}-columnmappingv2.csv`);
        let outputFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}-columnmappingv2-preprocessv2.csv`);
        let responseToSend: any = {
            msg: "File started processing",
            statusCode: 200
        }
        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });

            const customerModelDataPromise = customerModelsRepository.findOne({
                where: { email: email, vendor_list_url: uploadedFileName, status: 3 },
            });

            const [customerModelData, findbyemail] = await Promise.all([customerModelDataPromise, findbyemailPromise]);
            if (customerModelData) {
                await downloadFileFromS3IfNotAvailable(file, email);

                const scriptPath = path.join(__dirname, '../../python_models/lead_generation/preprocessv2.py');
                let args = ["--file_path", file, "--max_rows", "1000"];

                let python_output: any = await runPythonScript(scriptPath, args);
                const { output } = await extractPythonResponse({ python_output });
                // let output: ScriptOutput = JSON.parse(python_output.at(-1).replace(/'/g, '"'));
                if (output.success == 'True') {
                    // upload output file to s3
                    await UploadS3(outputFile, fs.createReadStream(outputFile), email);
                    customerModelData.status = 4;
                    await customerModelsRepository.update(customerModelData);
                    this.modelCreationService(customerRepository, customerModelsRepository, customerIndustryRepository,  email, uploadedFileName);
                }
                else {
                    customerModelData.error = "preprocess_error";
                    customerModelData.error_detail = output.error_details;
                    const optionsforAdminMail = {
                        error: customerModelData.error,
                        errorDetails: customerModelData.error_detail,
                        content: null
                    }
                    sendEmailToAdmin(uploadedFileName, findbyemail, customerRepository, optionsforAdminMail)
                    await customerModelsRepository.update(customerModelData);
                    responseToSend.msg = customerModelData.error;
                    responseToSend.statusCode = 400;
                }
            }
            else {
                responseToSend.msg = 'ERROR - customer-models.service#filePreProcessService, no model history found for file'
                responseToSend.statusCode = 400;
            }
        } catch (e) {
            responseToSend.msg = e.message;
            responseToSend.statusCode = 400;
        } finally {
            this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(file)
        }
        return responseToSend

    }


    static async modelCreationService(customerRepository: any, customerModelRepository: CustomerModelsRepository, customerIndustryRepository: CustomerIndustryRepository
         , email: string, uploadedFileName: string) {

        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let file = path.join(__dirname, `../../.sandbox/${filename_without_extension}-columnmappingv2-preprocessv2.csv`);
        let outputFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}-columnmappingv2-preprocessv2-modelcreationv2.csv`);
        let responseToSend: any = {
            msg: "File started processing",
            statusCode: 200
        }
        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });
            
            const customerModelDataPromise = customerModelRepository.findOne({
                where: { email: email, vendor_list_url: uploadedFileName, status: 4 },
            });

            const [customerModelData, findbyemail] = await Promise.all([customerModelDataPromise, findbyemailPromise]);
            if (customerModelData) {
                await downloadFileFromS3IfNotAvailable(file, email);
                const scriptPath = path.join(__dirname, '../../python_models/lead_generation/model_creationv2.py');
                let args = ["--file_path"];
                args.push(file);

                let python_output: any = await runPythonScript(scriptPath, args);
                const { output } = await extractPythonResponse({ python_output });
                // let output: ScriptOutput = JSON.parse(python_output.at(-1).replace(/'/g, '"'));
                // let output = { success: "True"}
                if (output.success == 'True') {
                    let fileStream = fs.createReadStream(outputFile);
                    await UploadS3(outputFile, fileStream, email);

                    // Read and parse the CSV file
                    const csvData: any = await new Promise((resolve, reject) => {
                        const parsedData: any = [];
                        fs.createReadStream(outputFile)
                            .pipe(csvParser())
                            .on('data', (row: any) => {
                                parsedData.push(row);
                            })
                            .on('end', () => {
                                resolve(parsedData);
                            })
                            .on('error', (error: any) => {
                                reject(error);
                            });
                    });

                    customerModelData.status = 5;
                    // let zipcode_sorted_list: any = {};
                    const zipcode_sorted_list = csvData.map((ele: any) => ({
                        [ele.zip]: ele.conversion_probability
                    }));
                    // csvData.forEach((ele: any) => {
                    // zipcode_sorted_list[ele.zip] = ele.conversion_probability;
                    // });
                    customerModelData.zipcode_sorted_list = zipcode_sorted_list;

                    await customerModelRepository.update(customerModelData);
                    this.criteriaGenerationService(customerRepository, customerModelRepository, customerIndustryRepository, email, uploadedFileName);
                }
                else {
                    customerModelData.error = "model_creation_error";
                    customerModelData.error_detail = output.error_details;
                    const optionsforAdminMail = {
                        error: customerModelData.error,
                        errorDetails: customerModelData.error_detail,
                        content: null
                    }
                    sendEmailToAdmin(uploadedFileName, findbyemail, customerRepository, optionsforAdminMail)
                    await customerModelRepository.update(customerModelData);
                    responseToSend.msg = customerModelData.error;
                    responseToSend.statusCode = 400;
                }
            } else {
                responseToSend.msg = 'ERROR - customer-models.service#modelCreationService, no model history found for file'
                responseToSend.statusCode = 400;
            }
        }
        catch (e) {
            responseToSend.msg = e.message;
            responseToSend.statusCode = 400;
        }
        finally {
            this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(file)
        }
        return responseToSend

    }

    static async criteriaGenerationService(customerRepository: any, customerModelRepository: CustomerModelsRepository, customerIndustryRepository: CustomerIndustryRepository,
        email: string, uploadedFileName: string) {
        const modelCreation = true;
        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let file = path.join(__dirname, `../../.sandbox/${filename_without_extension}-columnmappingv2-preprocessv2.csv`);
        let responseToSend: any = {}
        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });
            let customerModelDataPromise = customerModelRepository.findOne({
                where: { and: [{ email, vendor_list_url: uploadedFileName }] },
            });
            const customerIndustryDetailsPromise =  customerIndustryRepository.findOne({where:{email}})
            
            let [findbyemail, customerModelData, customerIndustryDetails] = await Promise.all([findbyemailPromise, customerModelDataPromise, customerIndustryDetailsPromise])
            if (customerModelData && findbyemail && customerIndustryDetails) {
                const industryType = customerIndustryDetails?.industry_type || ''
                await downloadFileFromS3IfNotAvailable(file, email);
                const scriptPath = path.join(__dirname, '../../python_models/lead_generation/criteria_generationv2.py');
                let args = ["--file_path"];
                args.push(file);

                let python_output: any = await runPythonScript(scriptPath, args);
                const { output } = await extractPythonResponse({ python_output });
                console.log("after calling criteria_generationv2.py and output is ", output)
                if (output.success == 'True') {
                    const criteria = output.criteria;
                    // const criteria: any = customerModelData.criteria;

                    customerModelData.status = 6;
                    responseToSend.message = 'Success'

                    const stateCodesArray = Object.keys(stateObj);

                    const melissaAgeSequence = GenerateLeadsService.findMelissaSequences(criteria.age, 'age')
                    // const melissaAgeSequence = 0;
                    // const melissaAgeSequence = GenerateLeadsService.findMelissaSequences([60,70], 'age')
                    const melissaHouseholdSequence = GenerateLeadsService.findMelissaSequences(criteria.income, 'household')
                    // const melissaHouseholdSequence = GenerateLeadsService.findMelissaSequences([60,70], 'household')
                    criteria.melissaAgeSequence = melissaAgeSequence;
                    criteria.melissaHouseholdSequence = melissaHouseholdSequence;
                    customerModelData.criteria = criteria;
                    const options = {
                        'callType': 'get',
                        'searchType': 'state',
                        'queryParams': {
                            'st': stateCodesArray.join(','),
                            // 'st': stateCodesArray[0],
                            'cAge-d': melissaAgeSequence,
                            'hInc-d': melissaHouseholdSequence
                        }
                    }
                    // calling melissa to get nation wide count
                    // console.log("after calling melissa sequence to get nation wide count", new Date())
                    // let melissaResponse: any = await GenerateLeadsService.melissaListApi(options)
                    // console.log("called melissa api to get nation wide count", new Date())
                    // let melissaResponse: any = null 
                    // const totalCount = melissaResponse?.data?.data?.Consumer?.TotalCount?.Count || 0;
                    // const nationWideCount = totalCount;
                    // const nationWideCount = "0";
                    // console.log("Melissa nation wide count response", melissaResponse?.data?.data?.Consumer)
                    // console.log("Melissa nation wide nationWideCount", nationWideCount)
                    // const topXZipCode = 2;
                    const topXZipCode = 40;
                    const top5Zipcodes = customerModelData?.zipcode_sorted_list?.map((ele: { [key: string]: any }) => {
                        const zipcode = Object.keys(ele)[0];
                        return zipcode.length < 5 ? '0'.repeat(5 - zipcode.length) + zipcode : zipcode;
                    })
                        .slice(0, topXZipCode) // Take the top X zip codes
                        .join(',');
                    const otherOptions = {
                        'callType': 'get',
                        'searchType': 'zip',
                        'queryParams': {
                            'zip': top5Zipcodes,
                            'cAge-d': melissaAgeSequence,
                            'hInc': melissaHouseholdSequence
                        }
                    }
                    // calling melissa to get leads count for x zipcode
                    console.log(`Time before calling melissa api with ${topXZipCode} zipcode: ${new Date()}`)
                    
                    let melissaResponse = await GenerateLeadsService.melissaListApi(otherOptions, industryType, modelCreation)
                    console.log(`Time after calling melissa api with ${topXZipCode} zipcode: ${new Date()}`)
                    const topXZipcodeCount = melissaResponse?.data?.data?.Consumer?.TotalCount?.Count || 0;
                    const nationWideCount = topXZipcodeCount;
                    // const topXZipcodeCount = "11111"
                    console.log(`Top ${topXZipCode} zipcode is ${topXZipcodeCount}`)
                    customerModelData.insights = {
                        nation_wide_count: nationWideCount,
                        top_zip_code: {
                            no_of_count: topXZipCode,
                            zipcode: top5Zipcodes,
                            leads_count: topXZipcodeCount
                        },
                        feature_columns: []
                    }

                    // if nation wide count is zero then set as error state and send email to admin
                    if (!parseInt(nationWideCount) || nationWideCount == "0") {
                        customerModelData.error = "leads_count_insufficient";
                        customerModelData.error_detail = `melissa reported back only ${nationWideCount} leads`;
                        customerModelData.status = 5
                        const optionsforAdminMail = {
                            error: customerModelData.error,
                            errorDetails: customerModelData.error_detail,
                            content: null
                        }
                        sendEmailToAdmin(uploadedFileName, findbyemail, customerRepository, optionsforAdminMail)
                    }
                    else {
                        // if count is there then send email to custoemr
                        const formattedNumber = nationWideCount ? nationWideCount.toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",") : 0;
                        const formattedNumberString = formattedNumber || formattedNumber.toString();

                        const optionsforAdminMail = {
                            model_name: customerModelData.name,
                            nationwide_potential_leads: formattedNumberString,
                            leads_counts: formattedNumberString
                        }
                        sendMailChimpEmail("lead_generation_model_ready", email, uploadedFileName, findbyemail.name, true, optionsforAdminMail);
                    };
                    await customerModelRepository.update(customerModelData);
                    responseToSend.error = 'Criteria generation process done';
                    responseToSend.statusCode = 200;

                }
                else {
                    customerModelData.error = "criteria_generation_error";
                    customerModelData.error_detail = output.error_details;
                    const optionsforAdminMail = {
                        error: customerModelData.error,
                        errorDetails: customerModelData.error_detail,
                        content: null
                    }
                    sendEmailToAdmin(uploadedFileName, findbyemail, customerRepository, optionsforAdminMail)
                    await customerModelRepository.update(customerModelData);
                    responseToSend.error = output.error_details;
                    responseToSend.statusCode = 500;
                }
            } else {
                responseToSend.error = "File History not found";
                responseToSend.statusCode = 500;
            }
        }
        catch (e) {
            console.error("error in criteria generation function", e)
            responseToSend.error = e.message;
            responseToSend.statusCode = 500;
        }
        finally {
            // this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(file)
        }
        return responseToSend

    }

    static async leadSortingModelCreationFileValidateService(customerRepository: CustomerRepository,
        customerModelsRepository: CustomerModelsRepository, customerIndustryRepository: CustomerIndustryRepository,
        email: string, uploadedFileName: string, modelDetails: any) {

        let { modelName, modelDescription, defaultModel, industryType, industryProfileId, featureColumns, moduleType } = modelDetails;
        industryType = industryType ?? industryTypes.REAL_ESTATE_INVESTORS;
        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let existingCustomerfile = path.join(__dirname, `../../.sandbox/${filename_without_extension}.csv`);
        const outputFileName = `${filename_without_extension}_rwr.csv`;
        let outputFilePath = path.join(__dirname, `../../.sandbox/${outputFileName}`);

        let responseToSend: any = {}
        try {
            const findByEmailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });
            const customerModelsPromise = customerModelsRepository.findOne({
                where: {
                    or: [
                        { and: [{ email: email }, { vendor_list_url: uploadedFileName }, { name: modelName }, { status: 1 }] }
                    ]
                }
            });
            const findIndustryPromise = customerIndustryRepository.findOne({
                where: { email },
            });
            const [findByEmail, customerModels, customerIndustry] = await Promise.all([findByEmailPromise, customerModelsPromise, findIndustryPromise]);
            if (findByEmail && customerModels && customerIndustry) {
                let findIndustryProfile = await customerIndustryRepository.findOne({ where: { email, industry_type: industryType } })
                let industrialProfile = [];
                const industryProfile = findIndustryProfile?.industry_profile || []
                let industrialProfileQuestionnaire: any = {};
                // if id is not coming from frontend then take default profile
                if (industryProfileId)
                    industrialProfile = industryProfile.filter((ele: { id: any; }) => ele.id == industryProfileId)
                else
                    industrialProfile = industryProfile.filter((ele: { default: any; }) => ele.default == true)
                await downloadFileFromS3IfNotAvailable(existingCustomerfile, email);
                // TODOO: pass investment profile
                let scriptPath = path.join(__dirname, '../../python_models/lead_sorting/column_mapping.py');

                let args = ["--file_path", existingCustomerfile, "--feature_columns", JSON.stringify({ "feature_columns": featureColumns })];
                if (industrialProfile?.length) {
                    industrialProfileQuestionnaire = industrialProfile && industrialProfile.length && industrialProfile[0].question_answers || {};
                    let industrialProfileToSend = { ...industrialProfileQuestionnaire, industry_type: industryType }
                    args.push("--industry_profile");
                    args.push(JSON.stringify(industrialProfileToSend));
                }
                // args.push(file);
                let python_output: any = await runPythonScript(scriptPath, args);
                let { output } = await extractPythonResponse({ python_output });

                customerModels.industry_profile = industrialProfileQuestionnaire;
                customerIndustry.industry_type = industryType;
                customerModels.insights = {
                    feature_columns: output.feature_columns,
                    nation_wide_count: '',
                    top_zip_code: {}
                }
                let existingFileStream = fs.createReadStream(existingCustomerfile);
                await UploadS3(existingCustomerfile, existingFileStream, email);


                if (output.success !== 'True') {
                    // customer_model_obj = await customerModelsRepository.create(customer_model_obj)
                    console.log("File processing ERROR: file-upload.service#fileValidateService");
                    const missingColoumns = output?.error_details?.split(":")[1]?.trim()?.split('.')[0];

                    const options = {
                        missing_columns: missingColoumns
                    }
                    const run = sendMailChimpEmail("Column_mapping_failed_00223_Ver_1", email, filename_without_extension, findByEmail.name, false, options);
                    // fileHistory.mapped_cols = mappedColumns;
                    customerModels.error = "insufficient_columns";
                    customerModels.error_detail = output.error_details;
                    responseToSend.error = customerModels.error;
                    const optionsforAdminMail = {
                        error: customerModels.error,
                        errorDetails: customerModels.error_detail,
                        content: null
                    }
                    sendEmailToAdmin(uploadedFileName, findByEmail, customerRepository, optionsforAdminMail)
                    await customerModelsRepository.update(customerModels);
                } else {

                    let outputFileStream = fs.createReadStream(outputFilePath);
                    await UploadS3(outputFilePath, outputFileStream, email);

                    customerModels.status = 2;
                    await customerModelsRepository.update(customerModels)
                    if (defaultModel) {
                        findByEmail.lead_sorting_default_model = {
                            model_id: customerModels.id || '',
                            model_name: customerModels.name
                        }
                        await customerRepository.update(findByEmail);
                    }
                    this.leadSortingModelCreationPreprocess(customerRepository, customerModelsRepository, customerIndustryRepository, email, uploadedFileName, modelDetails)
                }
            }
            else {
                responseToSend.error = 'ERROR - customer-models.service#fileValidateService, no customer record found for file'
            }
        } catch (e) {
            console.error("Error in fileValidateService", e.message)
            responseToSend.error = e.message;
        } finally {
            this.unLinkFileIfExists(existingCustomerfile)
            this.unLinkFileIfExists(outputFilePath)
        }
        return responseToSend

    }
    static async leadSortingModelCreationPreprocess(customerRepository: CustomerRepository, customerModelsRepository: CustomerModelsRepository,
        customerIndustryRepository: CustomerIndustryRepository, email: string, uploadedFileName: string, modelDetails: any) {

        const { modelName, defaultModel, featureColumns } = modelDetails;

        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let existingCustomerfile = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr.csv`);
        const outputFileName = `${filename_without_extension}_rwr_appended.csv`;
        let outputFilePath = path.join(__dirname, `../../.sandbox/${outputFileName}`);

        let responseToSend: any = {}
        try {
            const findByEmailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });
            const customerModelsPromise = customerModelsRepository.findOne({
                where: {
                    or: [
                        { and: [{ email: email }, { vendor_list_url: uploadedFileName }, { name: modelName }, { status: 2 }] }
                    ]
                }
            });
            const findIndustryPromise = customerIndustryRepository.findOne({
                where: { email },
            });
            const [findByEmail, customerModels, customerIndustry] = await Promise.all([findByEmailPromise, customerModelsPromise, findIndustryPromise]);
            if (findByEmail && customerModels) {

                await downloadFileFromS3IfNotAvailable(existingCustomerfile, email);
                let scriptPath = path.join(__dirname, '../../python_models/lead_sorting/preprocess.py');
                const args = [
                    "--file_path", existingCustomerfile,
                    "--max_rows", "1000",
                    "--data_type", "test",
                    "--feature_columns", JSON.stringify({ "feature_columns": featureColumns })
                ];
                const industrialProfile = customerModels.industry_profile || null;
                const industryType = customerIndustry?.industry_type ?? industryTypes.REAL_ESTATE_INVESTORS;
                if (industrialProfile) {
                    let industrialProfileToSend = { ...industrialProfile, industry_type: industryType }
                    args.push("--industry_profile");
                    args.push(JSON.stringify(industrialProfileToSend));
                }
                let python_output: any = await runPythonScript(scriptPath, args);
                const { output } = await extractPythonResponse({ python_output });

                let existingFileStream = fs.createReadStream(existingCustomerfile);
                await UploadS3(existingCustomerfile, existingFileStream, email);

                if (output?.success !== 'True') {

                    console.log("File processing ERROR: file-upload.service#leadSortingModelCreationPreprocess");
                    const missingColoumns = output?.error_details?.split(":")[1]?.trim()?.split('.')[0];

                    const options = {
                        missing_columns: missingColoumns
                    }
                    sendMailChimpEmail("Column_mapping_failed_00223_Ver_1", email, filename_without_extension, findByEmail.name, false, options);
                    customerModels.error = "preprocess Error";
                    customerModels.error_detail = output.error_details;
                    responseToSend.error = customerModels.error;
                    const optionsforAdminMail = {
                        error: customerModels.error,
                        errorDetails: customerModels.error_detail,
                        content: null
                    }
                    sendEmailToAdmin(uploadedFileName, findByEmail, customerRepository, optionsforAdminMail)
                    await customerModelsRepository.update(customerModels)
                } else {

                    let outputFileStream = fs.createReadStream(outputFilePath);
                    await UploadS3(outputFilePath, outputFileStream, email);

                    customerModels.status = 3;
                    await customerModelsRepository.update(customerModels)
                    this.leadSortingModelCreation(customerRepository, customerModelsRepository, email, uploadedFileName, modelDetails)
                }
            }
            else {
                responseToSend.error = 'ERROR - customer-models.service#leadSortingModelCreationPreprocess, no customer record found for file'
            }
        } catch (e) {
            console.error("Error in leadSortingModelCreationPreprocess", e.message)
            responseToSend.error = e.message;
        } finally {
            this.unLinkFileIfExists(existingCustomerfile)
            this.unLinkFileIfExists(outputFilePath)
        }
        return responseToSend

    }
    static async leadSortingModelCreation(customerRepository: CustomerRepository, customerModelsRepository: CustomerModelsRepository, email: string, uploadedFileName: string, modelDetails: any) {

        const { modelName, defaultModel } = modelDetails;

        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let trainFilePath = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended.csv`);

        const ohePikleFileName = `${filename_without_extension}_rwr_appended_ohe.pkl`;
        let ohePikleFilePath = path.join(__dirname, `../../.sandbox/${ohePikleFileName}`);

        const scalerPikleFileName = `${filename_without_extension}_rwr_appended_scaler.pkl`;
        let scalerPikleFilePath = path.join(__dirname, `../../.sandbox/${scalerPikleFileName}`);

        const pcaPikleFileName = `${filename_without_extension}_rwr_appended_pca.pkl`;
        let pcaPikleFilePath = path.join(__dirname, `../../.sandbox/${pcaPikleFileName}`);

        const vectorFileName = `${filename_without_extension}_rwr_appended_vectors.csv`;
        let vectorFilePath = path.join(__dirname, `../../.sandbox/${vectorFileName}`);

        let responseToSend: any = {}
        let filePaths = [trainFilePath, vectorFilePath, ohePikleFilePath, scalerPikleFilePath, pcaPikleFilePath]
        try {
            const findByEmailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });

            const customerModelsPromise = customerModelsRepository.findOne({
                where: {
                    or: [
                        { and: [{ email }, { vendor_list_url: uploadedFileName }, { name: modelName }, { status: 3 }] }
                    ]
                }
            });
            const [findByEmail, customerModels] = await Promise.all([findByEmailPromise, customerModelsPromise]);
            if (customerModels && findByEmail) {
                await downloadFileFromS3IfNotAvailable(trainFilePath, email);
                let scriptPath = path.join(__dirname, '../../python_models/lead_sorting/training_data_vectorizer.py');

                let args = ["--train_file_path", trainFilePath, "--feature_columns", []];
                let python_output: any = await runPythonScript(scriptPath, args);
                let { output } = await extractPythonResponse({ python_output });

                let trainFileStream = fs.createReadStream(trainFilePath);
                await UploadS3(trainFilePath, trainFileStream, email);


                if (output.success !== 'True') {
                    customerModels.error = "lead sorting";
                    customerModels.error_detail = output.error_details;
                    const optionsforAdminMail = {
                        error: customerModels.error,
                        errorDetails: customerModels.error_detail,
                        content: null
                    }
                    sendEmailToAdmin(uploadedFileName, findByEmail, customerRepository, optionsforAdminMail)
                    await customerModelsRepository.update(customerModels)
                } else {

                    let ohePikleFileStream = fs.createReadStream(ohePikleFilePath);
                    await UploadS3(ohePikleFilePath, ohePikleFileStream, email);

                    let scalerPikleFileStream = fs.createReadStream(scalerPikleFilePath);
                    await UploadS3(scalerPikleFilePath, scalerPikleFileStream, email);

                    let vectorFileStream = fs.createReadStream(vectorFilePath);
                    await UploadS3(vectorFilePath, vectorFileStream, email);

                    let pcaFileStream = fs.createReadStream(pcaPikleFilePath);
                    await UploadS3(pcaPikleFilePath, pcaFileStream, email);

                    customerModels.status = 6;
                    await customerModelsRepository.update(customerModels)

                    sendMailChimpEmail("lead_scoring_model_ready", email, uploadedFileName, findByEmail.name, true, { "model_name": customerModels.name });
                }
            }
            else {
                responseToSend.error = 'ERROR - customer-models.service#leadSortingModelCreation, no customer record found for file'
            }
        } catch (e) {
            console.error("Error in leadSortingModelCreation", e.message)
            responseToSend.error = e.message;
        } finally {
            // deleting files from local
            filePaths.forEach(ele => this.unLinkFileIfExists(ele))
        }
        return responseToSend

    }
    static async leadSortingFileValidateService(customerRepository: any, customerModelsRepository: CustomerModelsRepository, fileHistoryRepository: FileHistoryRepository, email: string, uploadedFileName: string, modelDetails: any) {
        let responseToSend: any = {};
        try {
            const { modelName } = modelDetails;

            const customerModelData = await customerModelsRepository.findOne({
                where: {
                    name: modelName,
                    type: "lead_sorting",
                    email
                }
            })
            if (customerModelData) {

                const LeadsFilenameWithoutExtension: string = uploadedFileName.split(".").slice(0, -1).join('.');
                let leadsCustomerFilePath = path.join(__dirname, `../../.sandbox/${LeadsFilenameWithoutExtension}.csv`);

                const outputFileName = `${LeadsFilenameWithoutExtension}_rwr.csv`;
                let outputFilePath = path.join(__dirname, `../../.sandbox/${outputFileName}`);

                try {
                    const findByEmail = await customerRepository.findOne({
                        fields: { login_history: false, file_history: false },
                        where: { email: email },
                    });
                    if (findByEmail) {
                        await downloadFileFromS3IfNotAvailable(leadsCustomerFilePath, email);
                        let scriptPath = path.join(__dirname, '../../python_models/lead_sorting/column_mapping.py');
                        const featureColumns = customerModelData?.insights?.feature_columns || [];
                        let args = ["--file_path", leadsCustomerFilePath, "--feature_columns", JSON.stringify({ "feature_columns": featureColumns })];
                        // let args = ["--file_path", leadsCustomerFilePath, "--feature_columns", feature_columns];
                        let python_output: any = await runPythonScript(scriptPath, args);

                        const { output } = await extractPythonResponse({ python_output });

                        let fileHistoryUpdateObj: any = new FileHistory({
                            email,
                            model_name: modelName,
                            filename: uploadedFileName,
                            upload_date: new Date(),
                            record_count: output?.rowCount,
                            status: 1,
                            file_extension: ".csv",
                            error_detail: output.error_details,
                            error: output.error
                        });

                        let leadsFileStream = fs.createReadStream(leadsCustomerFilePath);
                        await UploadS3(leadsCustomerFilePath, leadsFileStream, email);

                        if (output.success !== 'True') {

                            const missingColoumns = output?.error_details?.split(":")[1]?.trim()?.split('.')[0];

                            const options = {
                                missing_columns: missingColoumns
                            }
                            const run = sendMailChimpEmail("Column_mapping_failed_00223_Ver_1", email, uploadedFileName, findByEmail.name, false, options);
                            // fileHistory.mapped_cols = mappedColumns;
                            fileHistoryUpdateObj.error = "insufficient_columns";
                            fileHistoryUpdateObj.error_detail = output.error_details;
                            responseToSend.error = fileHistoryUpdateObj.error;
                            const optionsforAdminMail = {
                                error: fileHistoryUpdateObj.error,
                                errorDetails: fileHistoryUpdateObj.error_detail,
                                content: null
                            }
                            fileHistoryUpdateObj = await fileHistoryRepository.create(fileHistoryUpdateObj)
                            sendEmailToAdmin(uploadedFileName, findByEmail, customerRepository, optionsforAdminMail)

                        } else {
                            // uploading output file to s3
                            let outputFileStream = fs.createReadStream(outputFilePath);
                            await UploadS3(outputFilePath, outputFileStream, email);

                            fileHistoryUpdateObj.status = 2;
                            fileHistoryUpdateObj = await fileHistoryRepository.create(fileHistoryUpdateObj)
                            this.leadSortingPreprocess(customerRepository, customerModelsRepository, fileHistoryRepository, email, uploadedFileName, modelDetails)

                        }
                    }
                    else {
                        responseToSend.error = 'ERROR - customer-models.service#leadSortingFileValidateService, no customer record found for file'
                    }
                } catch (e) {
                    responseToSend.error = e.message;
                } finally {
                    this.unLinkFileIfExists(outputFilePath)
                    this.unLinkFileIfExists(leadsCustomerFilePath)
                }
            } else {
                responseToSend.error = "invalid model";
            }
        } catch (error) {
            responseToSend.error = error.message;
        }
        return responseToSend

    }

    static async leadSortingPreprocess(customerRepository: any, customerModelsRepository: CustomerModelsRepository, fileHistoryRepository: FileHistoryRepository, email: string, uploadedFileName: string, modelDetails: any) {
        let responseToSend: any = {};
        try {
            const { modelName } = modelDetails;

            const customerModelDataPromise = customerModelsRepository.findOne({
                where: {
                    name: modelName,
                    type: "lead_sorting",
                    email
                }
            })
            const fileHistoryPromise = fileHistoryRepository.findOne({
                where: {
                    or: [
                        { and: [{ email: email }, { filename: uploadedFileName }, { model_name: modelName }, { status: 2 }] }
                    ]
                }
            });
            const [customerModelData, fileHistory] = await Promise.all([customerModelDataPromise, fileHistoryPromise]);
            if (customerModelData && fileHistory) {

                const LeadsFilenameWithoutExtension: string = uploadedFileName.split(".").slice(0, -1).join('.');
                let leadsCustomerFilePath = path.join(__dirname, `../../.sandbox/${LeadsFilenameWithoutExtension}_rwr.csv`);

                const outputFileName = `${LeadsFilenameWithoutExtension}_rwr_appended.csv`;
                let outputFilePath = path.join(__dirname, `../../.sandbox/${outputFileName}`);


                try {
                    const findByEmail = await customerRepository.findOne({
                        fields: { login_history: false, file_history: false },
                        where: { email: email },
                    });
                    if (findByEmail) {
                        await downloadFileFromS3IfNotAvailable(leadsCustomerFilePath, email);
                        let scriptPath = path.join(__dirname, '../../python_models/lead_sorting/preprocess.py');
                        const featureColumns = customerModelData?.insights?.feature_columns || [];
                        let args = ["--file_path", leadsCustomerFilePath, "--feature_columns", JSON.stringify({ "feature_columns": featureColumns })];
                        let python_output: any = await runPythonScript(scriptPath, args);
                        const { output } = await extractPythonResponse({ python_output });

                        let leadsFileStream = fs.createReadStream(leadsCustomerFilePath);
                        await UploadS3(leadsCustomerFilePath, leadsFileStream, email);

                        if (output.success !== 'True') {

                            // const missingColoumns = output?.error_details?.split(":")[1]?.trim()?.split('.')[0];
                            // const options = {
                            //     missing_columns: missingColoumns
                            // }
                            // const run = sendMailChimpEmail("Column_mapping_failed_00223_Ver_1", email, uploadedFileName, findByEmail.name, false, options);
                            // fileHistory.mapped_cols = mappedColumns;
                            fileHistory.error = "preprocess_error";
                            fileHistory.error_detail = output.error_details;
                            responseToSend.error = fileHistory.error;
                            const optionsforAdminMail = {
                                error: fileHistory.error,
                                errorDetails: fileHistory.error_detail,
                                content: null
                            }
                            sendEmailToAdmin(uploadedFileName, findByEmail, customerRepository, optionsforAdminMail)
                            await fileHistoryRepository.update(fileHistory)
                        } else {
                            // uploading output file to s3
                            let outputFileStream = fs.createReadStream(outputFilePath);
                            await UploadS3(outputFilePath, outputFileStream, email);
                            fileHistory.status = 3;
                            await fileHistoryRepository.update(fileHistory)
                            this.leadSorting(customerRepository, customerModelsRepository, fileHistoryRepository, email, uploadedFileName, modelDetails)

                        }
                    }
                    else {
                        responseToSend.error = 'ERROR - customer-models.service#leadSortingPreprocess, no customer record found for file'
                    }
                } catch (e) {
                    responseToSend.error = e.message;
                } finally {
                    this.unLinkFileIfExists(outputFilePath)
                    this.unLinkFileIfExists(leadsCustomerFilePath)
                }
            } else {
                responseToSend.error = "invalid model";
            }
        } catch (error) {
            responseToSend.error = error.message;
        }
        return responseToSend

    }

    static async leadSorting(customerRepository: any, customerModelsRepository: CustomerModelsRepository, fileHistoryRepository: FileHistoryRepository, email: string, uploadedFileName: string, modelDetails: any) {
        let responseToSend: any = {};
        try {
            const { modelName } = modelDetails;
            let filePaths: any = [];
            const customerModelDataPromise = customerModelsRepository.findOne({
                where: {
                    name: modelName,
                    type: "lead_sorting",
                    email
                }
            })

            const fileHistoryPromise = fileHistoryRepository.findOne({
                where: {
                    or: [
                        { and: [{ email: email }, { filename: uploadedFileName }, { model_name: modelName }, { status: 3 }] }
                    ]
                }
            });
            const [customerModelData, fileHistory] = await Promise.all([customerModelDataPromise, fileHistoryPromise]);
            if (fileHistory && customerModelData) {
                const originalFile = customerModelData.vendor_list_url;
                const filename_without_extension: string = originalFile.split(".").slice(0, -1).join('.');
                let trainFilePath = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended.csv`);

                const LeadsFilenameWithoutExtension: string = uploadedFileName.split(".").slice(0, -1).join('.');
                let leadsCustomerFilePath = path.join(__dirname, `../../.sandbox/${LeadsFilenameWithoutExtension}_rwr_appended.csv`);

                const ohePikleFileName = `${filename_without_extension}_rwr_appended_ohe.pkl`;
                let ohePikleFilePath = path.join(__dirname, `../../.sandbox/${ohePikleFileName}`);

                const scalerPikleFileName = `${filename_without_extension}_rwr_appended_scaler.pkl`;
                let scalerPikleFilePath = path.join(__dirname, `../../.sandbox/${scalerPikleFileName}`);

                const pcaPikleFileName = `${filename_without_extension}_rwr_appended_pca.pkl`;
                let pcaPikleFilePath = path.join(__dirname, `../../.sandbox/${pcaPikleFileName}`);

                const vectorFileName = `${filename_without_extension}_rwr_appended_vectors.csv`;
                let vectorFilePath = path.join(__dirname, `../../.sandbox/${vectorFileName}`);

                // final output file
                const outputFileName = `${LeadsFilenameWithoutExtension}_rwr_appended_processed_betty.csv`;
                let outputFilePath = path.join(__dirname, `../../.sandbox/${outputFileName}`);

                try {
                    const findByEmail = await customerRepository.findOne({
                        fields: { login_history: false, file_history: false },
                        where: { email: email },
                    });
                    if (findByEmail) {
                        filePaths = [
                            leadsCustomerFilePath,
                            trainFilePath,
                            ohePikleFilePath,
                            scalerPikleFilePath,
                            vectorFilePath,
                            pcaPikleFilePath,
                            outputFilePath
                        ];
                        // download all files
                        await downloadFilesFromS3(filePaths, email)
                        let scriptPath = path.join(__dirname, '../../python_models/lead_sorting/vectorize_test_data.py');
                        const featureColumns = customerModelData?.insights?.feature_columns || [];

                        let args = ["--test_file_path", leadsCustomerFilePath, "--train_file_path", trainFilePath, "--train_vectors_path", vectorFilePath, "--feature_columns", JSON.stringify({ "feature_columns": featureColumns })];
                        // args.push(file);
                        let python_output: any = await runPythonScript(scriptPath, args);
                        const { output } = await extractPythonResponse({ python_output });

                        let leadsFileStream = fs.createReadStream(leadsCustomerFilePath);
                        await UploadS3(leadsCustomerFilePath, leadsFileStream, email);

                        if (output.success !== 'True') {
                            fileHistory.error = "lead sorting";
                            fileHistory.error_detail = output.error_details;
                            const optionsforAdminMail = {
                                error: fileHistory.error,
                                errorDetails: fileHistory.error_detail,
                                content: null
                            }
                            sendEmailToAdmin(uploadedFileName, findByEmail, customerRepository, optionsforAdminMail)
                            await fileHistoryRepository.update(fileHistory);
                        } else {

                            // uploading output file to s3
                            let outputFileStream = fs.createReadStream(outputFilePath);
                            await UploadS3(outputFilePath, outputFileStream, email);

                            fileHistory.status = 7;
                            const expireTime = 2 * 24 * 60 * 60;
                            const fileToDownload = generatePresignedS3Url(outputFilePath, email, expireTime)

                            let totalRows = -1;
                            let totalRowAbove100 = 0;
                            let totalRowsBelow100 = -1;
                            const processFile = async (filePath: string) => {
                                const parser = await fs.createReadStream(filePath).pipe(parse({ relax_quotes: true, skip_records_with_empty_values: true }));
                                for await (const _record of parser) {
                                    totalRows++;
                                    if (_record[_record.length - 1] > 100) totalRowAbove100++;
                                    else totalRowsBelow100++;
                                }
                            };
                            await processFile(outputFilePath);
                            let savingAmountFactor = industrTypesMetaData[findByEmail.industry_type]?.savings_calculation_factor || 0.67;;
                            let savingAmount = (totalRowsBelow100 * savingAmountFactor).toFixed(2);
                            const options = {
                                savings_amount: savingAmount.toLocaleString(),
                                download_url: fileToDownload
                            }
                            fileHistory.rows_below_100 = totalRowsBelow100;
                            const run = sendMailChimpEmail("done_betty_00223_ver_1", email, uploadedFileName, findByEmail.name, false, options);
                            await fileHistoryRepository.update(fileHistory);
                        }
                    }
                    else {
                        responseToSend.error = 'ERROR - customer-models.service#leadSortingModelCreation, no customer record found for file'
                    }
                } catch (e) {
                    console.error("Error in customer-model.service#leadSorting", e.message)
                    responseToSend.error = e.message;
                } finally {
                    filePaths.forEach((ele: any) => this.unLinkFileIfExists(ele))
                }

            } else {
                responseToSend.error = 'ERROR - customer-model.service#leadSorting, no file history or no model found for file'
            }
        } catch (error) {
            responseToSend.error = error.message;
        }
        return responseToSend

    }

}
