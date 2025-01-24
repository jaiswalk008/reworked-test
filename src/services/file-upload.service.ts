import {
    BindingScope,
    config,
    ContextTags,
    injectable,
    Provider,
} from '@loopback/core';
import { Request } from '@loopback/rest';
import multer from 'multer';
import { FILE_UPLOAD_SERVICE } from '../keys';
import { FileUploadHandler } from '../types';
import path from "path";
import fs, { createReadStream } from "fs";
import { downloadFileFromS3, runPythonScript, UploadS3, downloadFileFromS3IfNotAvailable, HubspotService } from '../services';
import { parse } from "csv-parse";
import { CustomerRepository, FileHistoryRepository, TransactionHistoryRepository, AdminEventsRepository, CustomerIndustryRepository, IntegrationsRepository, GenerateLeadsRepository } from "../repositories";
var csv = require("csv-parser");
import { sendMailChimpEmail, calculateRowsLeftForUser, sendEmailToAdmin, sendFileToCallback, getPriceFromRange, checkforCustomBranding, parseFileToJSON, updateleadGenerationData } from "../helper"
import Process from 'process'
import { extractPythonResponse } from "../helper/utils";
import { Customer, CustomerIndustry, FileHistory } from '../models';
import { CustomerService } from "../services/customer.service";
import { stripePayment } from '../helper/stripe-payment';
import { getFilterSort } from '../helper/filter-sort';
import { apiUsers } from '../constant/api_users';
import { sendTwit } from "../helper/integrations";
import { industryTypes, industrTypesMetaData} from "../constant/industry_type";
import { platformIntegrations } from "../constant/platform_integrations";

import { addOns } from "../constant/add_ons";
import { ZohoService } from './zoho.service';
import { uploadFileToGoogleFolder } from '../helper/google-drive';
import { usageType } from '../constant/usage_type';

/**
 * A provider to return an `Express` request handler from `multer` middleware
 */
@injectable({
    scope: BindingScope.TRANSIENT,
    tags: { [ContextTags.KEY]: FILE_UPLOAD_SERVICE },
})
export class FileUploadProvider implements Provider<FileUploadHandler> {

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

    /**
 * Get files and fields for the request
 * @param request - Http request
 */
    static async getFileHistoryPaginatedService(
        request: Request,
        customerRepository: any,
        fileHistoryRepository: FileHistoryRepository
    ) {
        const findbyemail = await customerRepository.find({
            fields: { login_history: false, file_history: false },
            where: { email: request.headers.email },
        });
        if (findbyemail.length <= 0 || !request.headers.email)
            return { result: "Email not found" };

        let where: any = { model_name: { exists: true, neq: null } }
        let order: string = 'upload_date DESC'

        const email: any = request.query.email;
        const sortName: any = request.query.sortName;
        const type: any = request.query.type;
        const page: any = request.query.page || 1;

        if (email) {
            where['email'] = email
        }


        const filtertype = "v1"
        const filterSort = getFilterSort({ where, filtertype, sortName, type })

        where = filterSort.where
        if (filterSort.order !== '') {
            order = filterSort.order
        }

        const limit = 10;
        const offset = (page - 1) * limit;

        const totalCountPromise = await fileHistoryRepository.count(where);

        let previous_files = await fileHistoryRepository.find({ where: where, order: [order], limit: limit, skip: offset });

        const [totalCount, data] = await Promise.all([totalCountPromise, previous_files]);
        const length = totalCount?.count

        // return previous_files
        return {
            length: length,
            data: data
        };
    }

    static async fileValidateService(customerRepository: CustomerRepository, fileHistoryRepository: FileHistoryRepository,
        transactionHistoryRepository: TransactionHistoryRepository, adminEventsRepository: AdminEventsRepository, customerIndustryRepository: CustomerIndustryRepository,
        integrationsRepository: IntegrationsRepository, generateLeadsRepository:GenerateLeadsRepository,email: string, uploadedFileName: string, columnMapping?: string, apiUser: Boolean = false ) {

        const responseToSend = {
            statusCode: 400,
            msg: "Invalid request",
            error: ''
        }
        let errorType = null;
        console.log(uploadedFileName)
        let file = path.join(__dirname, `../../.sandbox/${uploadedFileName}`);
        // Path to and name of object. For example '../myFiles/index.js'.
        const fileFormat: string = file.split(".").pop()!;
        const filename: string = uploadedFileName.split(".").slice(0, -1).join('.')
        let outputFile = path.join(__dirname, `../../.sandbox/${filename}_rwr.csv`)
        let findbyemail, fileHistory, customerIndustry; // Declare variables outside of the try block

        try {
            
            // Parallelize database lookups
            [findbyemail, fileHistory, customerIndustry] = await Promise.all([
                customerRepository.findOne({ fields: { login_history: false, file_history: false }, where: { email: email } }),
                fileHistoryRepository.findOne({ where: { and: [{ email }, { filename: uploadedFileName }, { status: 2 }] } }), 
                customerIndustryRepository.findOne({ where: { email } })
            ]);
            // Check if customer and file history exist
            if (!findbyemail) throw new Error("ERROR - file-upload.service#fileValidateService, no user found for email");
            if (!fileHistory) throw new Error("ERROR - file-upload.service#fileValidateService, no file history found for file");
            let totalAllowedRowCount, remainingRowsForMonth = 0;
            const pricingPlan = findbyemail.pricing_plan?.plan?.toUpperCase();
            ({ totalAllowedRowCount, remainingRowsForMonth } = (await calculateRowsLeftForUser(findbyemail, fileHistoryRepository)));
            if (!pricingPlan && fileHistory?.source !== usageType.LEADGENERATION) {
                if(totalAllowedRowCount < fileHistory.record_count){
                    errorType = 'no_plan_found';
                    // this.sendErrorMailAndUpdateFileHistory('no_plan_found', `ERROR - file-upload.service#fileValidateService, No plan found for ${email}`, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                    throw new Error("ERROR - file-upload.service#fileValidateService, No plan found")
                }
            }
            await downloadFileFromS3IfNotAvailable(file, email)

            let rowCount: number = 0;
            if (
                fileFormat.includes("csv") ||
                fileFormat.includes("xlsx") ||
                fileFormat.includes("CSV") ||
                fileFormat.includes("XLSX")
            ) {
                // fileHistory.record_count = 100;
                rowCount = fileHistory.record_count;
                // rowCount = 100;
                if (pricingPlan != 'POSTPAID' && fileHistory?.source !== usageType.LEADGENERATION ) {
                    if (totalAllowedRowCount !== undefined && rowCount > totalAllowedRowCount) {
                        fileHistory.error = "row_count_exceeds";
                        let success = false;

                        if (apiUser) {
                            let extraCredits = rowCount - totalAllowedRowCount;
                            // extraCredits = 1000;
                            // Retrieve price per row for the customer
                            const responseFromFunction = await CustomerService.getPerUnitPriceForCustomer(email, customerRepository, customerIndustryRepository);
                            if (responseFromFunction?.data?.perUnitCostAndRange) {
                                // Calculate total amount based on extra credits
                                let finalAmount = await getPriceFromRange(extraCredits, responseFromFunction.data.perUnitCostAndRange);
                                if (finalAmount) {
                                    // finalAmount = Math.round(finalAmount); // Round off to nearest integer

                                    // Metadata for transaction
                                    const metaData = {
                                        "no_of_credits": extraCredits,
                                        "total_cost": finalAmount,
                                        "source_type": 'lead_sorting',
                                        "leads_to_add": false,
                                    }

                                    // Arguments for payment processing
                                    const args = {
                                        totalAmount: finalAmount, metaData, noOfCredits: extraCredits, email,
                                        payment_type: "Bought credits for Lead Sorting",
                                        invoiceItemDescription: "Bought credits for Lead Sorting",
                                        sourceType: 'lead_sorting', leadsToAdd: false,
                                    };

                                    // Process payment and handle response
                                    if (findbyemail?.pricing_plan?.stripe_payment_method_id) {

                                        const response = await stripePayment(args, findbyemail, customerRepository, transactionHistoryRepository, adminEventsRepository);
                                        if (response && response.statusCode === 200) {
                                            success = true;
                                            findbyemail.row_credits = 0;
                                            await customerRepository.update(findbyemail);
                                        } else {
                                            fileHistory.error = 'stripe_payment_error';
                                            fileHistory.error_detail = response?.msg;
                                        }
                                    } else {
                                        fileHistory.error = 'stripe_payment_method_id_not_found';
                                        fileHistory.error_detail = 'Payment Method is null';
                                    }
                                }
                            }
                        }

                        if (!success) {
                            // Send email notification for row count exceedance
                            const options = { row_left: totalAllowedRowCount, row_required: rowCount };
                            await sendMailChimpEmail("row_count_failed_00223_Ver_1", email, filename, findbyemail.name, false, options);
                            await fileHistoryRepository.updateById(fileHistory.id, fileHistory);
                            responseToSend.error = 'Row Count Exceeded from remaining limit in your plan';
                            return responseToSend;
                        }
                    }
                }
                let columnMappingFileName = 'column_mapping.py';
                if(customerIndustry && customerIndustry.industry_type == industryTypes.SOLAR_INSTALLER && fileHistory?.source !== usageType.LEADGENERATION){
                    const freeCreditsLeft  = findbyemail.row_credits - rowCount;
                    if(freeCreditsLeft<=0 && !findbyemail.pricing_plan){
                        sendMailChimpEmail("Low_Credits_Ver_1",findbyemail.email,'',findbyemail.name);
                    }
                }
                if (customerIndustry && customerIndustry.industry_type == industryTypes.INSURANCE_PROVIDER)
                    columnMappingFileName = 'column_mapping_insurance.py';
                // await this.downloadResultService(customerRepository, fileHistoryRepository, email, uploadedFileName);
                const scriptPath = path.join(__dirname, `../../python_models/${columnMappingFileName}`);
                let args = ["--file_path"];
                args.push(file);
                if (columnMapping) {
                    args.push("--custom_mapping");
                    args.push(columnMapping);
                }

                if (fileHistory.industry_profile) {
                    args.push("--industry_profile");
                    args.push(JSON.stringify(fileHistory.industry_profile));
                }
                let python_output: any = await runPythonScript(scriptPath, args);
                const { output } = await extractPythonResponse({ python_output });
                console.log(output);
                if (output?.mapped_cols && Object.keys(output.mapped_cols).length > 0) {
                    fileHistory.mapped_cols = output.mapped_cols;
                }
                // fileHistory.mapped_cols = output?.mapped_cols
                if (output.success == 'True') {
                    responseToSend.statusCode = 200;
                    console.log("File processing success: file-upload.service#fileValidateService");
                    // We need to reduce the credits in case the customer has exceeded subscription usage for the month
                    if (pricingPlan != 'POSTPAID' && fileHistory?.source !== usageType.LEADGENERATION) {
                        if ((rowCount >= remainingRowsForMonth) && (findbyemail.row_credits)) {
                            findbyemail.row_credits = findbyemail.row_credits - (rowCount - remainingRowsForMonth);
                            // findbyemail[0].roll_over_credits = Math.max(0, (findbyemail[0].roll_over_credits ?? 0) - (rowCount - remainingRowsForMonth));
                            await customerRepository.update(findbyemail);
                        }
                    }
                    let fileStream = fs.createReadStream(outputFile);
                    await UploadS3(outputFile, fileStream, email);
                    fileHistory.status = 3;
                    await fileHistoryRepository.update(fileHistory);
                    this.filePreProcessService(customerRepository, fileHistoryRepository, customerIndustryRepository, integrationsRepository,generateLeadsRepository, email, uploadedFileName);
                    responseToSend.msg = 'Validation done successfully';
                }
                else {
                    console.log("File processing ERROR: file-upload.service#fileValidateService");
                    const missingColoumns = output?.error_details?.split(":")[1]?.trim()?.split('.')[0];
                    if(fileHistory.source ===usageType.LEADGENERATION){
                        updateleadGenerationData(email,findbyemail.name,uploadedFileName,'',{msg:"File processing error",details:`insufficient_columns: ${missingColoumns}`});
                    }
                    const options = {
                        missing_columns: missingColoumns
                    }
                    const run = sendMailChimpEmail("Column_mapping_failed_00223_Ver_1", email, filename, findbyemail.name, false, options);
                    this.sendErrorMailAndUpdateFileHistory('insufficient_columns', output.error_details, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                    responseToSend.error = 'insufficient_columns';
                }


            } else {
                responseToSend.error = 'Invalid file';
            }
        } catch (error) {
            responseToSend.statusCode = 400;
            responseToSend.error = errorType || error.message;
            responseToSend.msg = "error in fileValidateService"
            this.sendErrorMailAndUpdateFileHistory(responseToSend.error, responseToSend.msg, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)

        } finally {
            this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(file)
        }
        return responseToSend;
    }

    static async filePreProcessService(customerRepository: any, fileHistoryRepository: FileHistoryRepository, customerIndustryRepository: CustomerIndustryRepository, 
        integrationsRepository: IntegrationsRepository,generateLeadsRepository:GenerateLeadsRepository ,email: string, uploadedFileName: string) {
        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let file = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr.csv`);
        let outputFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended.csv`);
        let responseToSend: any = {}
        let fileHistory: FileHistory | null | undefined;
        let findbyemail: Customer | null | undefined;
        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email: email },
            });


            let fileHistoryPromise = fileHistoryRepository.findOne({
                where: {
                    or: [
                        { and: [{ email }, { filename: uploadedFileName }, { status: 3 }] },
                        { and: [{ email }, { filename: uploadedFileName }, { status: 2 }] }
                    ]
                }
            });
            [fileHistory, findbyemail] = await Promise.all([fileHistoryPromise, findbyemailPromise]);
            if (fileHistory && findbyemail) {
                await downloadFileFromS3IfNotAvailable(file, email);
                const scriptPath = path.join(__dirname, '../../python_models/preprocess.py');
                let args = ["--file_path"];
                args.push(file);
                let dataSource = "NonMelissa";
                if (dataSource) {
                    args.push("--data_source")
                    args.push(JSON.stringify(dataSource))
                }

                if (fileHistory.industry_profile) {
                    args.push("--industry_profile");
                    args.push(JSON.stringify(fileHistory.industry_profile));
                }
                if (process.env.MELISSA_KEY) {
                    console.log("file-upload.service#filePreProcessService Using Melissa")
                    dataSource = "Melissa";
                }

                let python_output: any = await runPythonScript(scriptPath, args);
                // console.log("File-Upload Service, #filePreProcessService, python_output", JSON.stringify(python_output));
                let { output } = await extractPythonResponse({ python_output });
                if (output.success == 'True') {
                    let fileStream = fs.createReadStream(outputFile);
                    await UploadS3(outputFile, fileStream, email);
                    fileHistory.status = 4;
                    await fileHistoryRepository.update(fileHistory);
                    this.bettyProcessingService(customerRepository, fileHistoryRepository, customerIndustryRepository, integrationsRepository,generateLeadsRepository, email, uploadedFileName);
                    responseToSend.message = 'Success'
                }
                else {
                    responseToSend.message = 'preprocess_error';
                    responseToSend.error = output.error_details;
                    this.sendErrorMailAndUpdateFileHistory(responseToSend.message, responseToSend.error, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                    if(fileHistory.source ===usageType.LEADGENERATION){
                        updateleadGenerationData(email,findbyemail.name,uploadedFileName,'',{msg:"preprocess_error",details:output.error_details});
                    }
                    return;
                }
            }
            else {
                responseToSend.error = 'ERROR - file-upload.service#filePreProcessService, no file history found for file'
                //throw new HttpErrors.NotFound("File History not found");
            }
        } catch (e) {
            responseToSend.error = e.message;
            responseToSend.msg = "error in filePreProcessService"
            this.sendErrorMailAndUpdateFileHistory(responseToSend.msg, responseToSend.error, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
        } finally {
            this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(file)
        }
        return responseToSend

    }

    static async fileContentService(request: Request, customerRepository: any, fileHistoryRepository: FileHistoryRepository) {
        let email = request.headers.email as string;
        let filename = request.headers.filename;
        let file = path.join(__dirname, `../../.sandbox/${filename}`);

        try {

            let data: Array<string> = [];
            await downloadFileFromS3IfNotAvailable(file, email);
            var fileData: any = await new Promise(function (resolve, reject) {
                fs.createReadStream(file)
                    .pipe(csv({}))
                    .on('data', (row: any) => {
                        if (data.length <= 3) {
                            data.push(row);

                        }

                    })
                    .on('end', () => {
                        resolve(data);

                    });
            });
            let responseData: any = { fileData, mapped_cols: {} }
            const fileHistory = await fileHistoryRepository.findOne({ where: { filename: filename as string, email: email } })
            if (fileHistory?.mapped_cols) responseData.mapped_cols = fileHistory.mapped_cols;
            return responseData;
        } catch (e) {
            console.error("error in fileContentService", e)
            throw Error(e)
        }
        finally {
            this.unLinkFileIfExists(file)
        }


    }

    static async bettyProcessingService(customerRepository: CustomerRepository, fileHistoryRepository: FileHistoryRepository, customerIndustryRepository: CustomerIndustryRepository, integrationsRepository: IntegrationsRepository, 
        generateLeadsRepository:GenerateLeadsRepository,email: string, uploadedFileName: string) {

        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let file = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended.csv`);
        let outputFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended_betty.csv`);
        let responseToSend: any = {}
        let fileHistory: FileHistory | null | undefined;
        let findbyemail: Customer | null | undefined;

        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email },
            });
            let fileHistoryPromise = fileHistoryRepository.findOne({
                where: {
                    or: [
                        { and: [{ email }, { filename: uploadedFileName }, { status: 4 }] },
                        { and: [{ email }, { filename: uploadedFileName }, { status: 3 }] }
                    ]
                }
            });
            [fileHistory, findbyemail] = await Promise.all([fileHistoryPromise, findbyemailPromise]);
            if (fileHistory && findbyemail) {
                const industryType = fileHistory.industry_profile.industryType || industryTypes.REAL_ESTATE_INVESTORS;
                await downloadFileFromS3IfNotAvailable(file, email);
                let args = ["--file_path"];
                args.push(file);
                let pythonFile = industrTypesMetaData[industryType].ml_file;
                if(findbyemail?.add_ons?.[addOns.CUSTOM_MODEL]?.ml_file) pythonFile =  findbyemail?.add_ons?.[addOns.CUSTOM_MODEL]?.ml_file;
                if (fileHistory.industry_profile) {
                    args.push("--industry_profile");
                    args.push(JSON.stringify(fileHistory.industry_profile));
                }

                const scriptPath = path.join(__dirname, `../../python_models/${pythonFile}.py`);
                let python_output: any = await runPythonScript(scriptPath, args);
                let { output } = await extractPythonResponse({ python_output });

                if (output.success == 'True') {

                    // calculate percentage_total_below_100
                    let totalRows = -1;
                    let totalRowAbove100 = 0;
                    let totalRowsBelow100 = -1;
                    let percentageTotalBelow100 = 0;
                    let medianFactor = industrTypesMetaData[industryType].threshold;
                    const processFile = async (filePath: string) => {
                        const parser = await fs.createReadStream(filePath).pipe(parse({ relax_quotes: true, skip_records_with_empty_values: true }));
                        for await (const _record of parser) {
                            totalRows++;
                            if (_record[_record.length - 1] > medianFactor) totalRowAbove100++;
                            else totalRowsBelow100++;
                        }
                    };

                    await processFile(outputFile);
                    percentageTotalBelow100 = Math.round((100 / totalRows) * (totalRows - totalRowAbove100)) || 0;

                    
                    const confidenceInsights = output?.ConfidenceDict || {};
                    // Create a new object with the required fields
                    const transformedConfidenceObject = {
                        confidence_score: confidenceInsights.confidence_score,
                        actual_ml_calling_count: confidenceInsights.actual_ml_calling_count, // Use value from the input JSON
                        full_name_coverage: confidenceInsights.full_name_missing_percentage, 
                        zipcode_coverage: confidenceInsights.dist_btw_site_mail_zip_percentage, 
                        age_source_melissa: confidenceInsights.potential_age_above_threshold_percentage || confidenceInsights.age_source_melissa, 
                        demo_address_verification_failed_percentage: confidenceInsights.demo_address_verification_failed_percentage,
                        demo_currently_lives_in_address_percentage: confidenceInsights.demo_currently_lives_in_address_percentage,
                        final_confidence_score: confidenceInsights.final_confidence_score,
                        // final_confidence_score: 10,
                        percentage_total_below_threshold: percentageTotalBelow100 || 0 
                    };

                    fileHistory.rows_below_100 = totalRowsBelow100;
                    
                    let fileStream = fs.createReadStream(outputFile);
                    await UploadS3(outputFile, fileStream, email);
                    fileHistory.status = 5;
                    fileHistory.confidence_insights = transformedConfidenceObject;
                    await fileHistoryRepository.update(fileHistory);
                    this.postProcessingValidationService(customerRepository, fileHistoryRepository, customerIndustryRepository, integrationsRepository,email, uploadedFileName, false);
                    responseToSend.message = 'Success'
                }
                else {
                    await this.sendErrorMailAndUpdateFileHistory('betty_error', output.error_details, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                    if(fileHistory.source ===usageType.LEADGENERATION){
                        updateleadGenerationData(email,findbyemail.name,uploadedFileName,'',{msg:"betty_error",details:output.error_details});
                    }
                    return;
                }
            } else {
                responseToSend.error = "File History or email not found";
            }
        }
        catch (e) {
            responseToSend.error = e.message;
            responseToSend.msg = "error in bettyProcessingService"
            await this.sendErrorMailAndUpdateFileHistory(responseToSend.msg, responseToSend.error, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
        }
        finally {
            this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(file)
        }
        return responseToSend

    }

    static async postProcessingValidationService(customerRepository: any, fileHistoryRepository: FileHistoryRepository, customerIndustryRepository: CustomerIndustryRepository, integrationsRepository: IntegrationsRepository,
        email: string, uploadedFileName: string, isAdmin: boolean = false) {
        
        let statusCode = 400;
        let msg = "Invalid request";
        let error = '';
        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        let outputFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended_betty.csv`);
        let fileHistory: FileHistory | null | undefined;
        let findbyemail: Customer | null | undefined;
        let customerIndustry: CustomerIndustry | null | undefined;

        try {
            const findbyemailPromise = customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email },
            });
            const findIndustryPromise = customerIndustryRepository.findOne({
                where: { email },
            });
            let fileHistoryPromise = fileHistoryRepository.findOne({
                where: { and: [{ email }, { filename: uploadedFileName }, { status: 5 }] },
            });
            [fileHistory, findbyemail, customerIndustry] = await Promise.all([fileHistoryPromise, findbyemailPromise, findIndustryPromise]);
            if (fileHistory && findbyemail && customerIndustry) {
                let skipThresholdCheck = false;
                let userLevelAddOn = findbyemail?.add_ons?.[addOns.DISABLE_POST_PROCESS_VALIDATION] || false;
                if (isAdmin) {
                    skipThresholdCheck = true;
                } else if (userLevelAddOn) {
                    skipThresholdCheck = userLevelAddOn;
                } else if (industrTypesMetaData?.[customerIndustry.industry_type]?.skip_threshold_check) {
                    skipThresholdCheck = industrTypesMetaData[customerIndustry.industry_type].skip_threshold_check;
                }

                if ((fileHistory?.record_count && fileHistory.record_count > 10) && (!(isAdmin))  && !skipThresholdCheck) {
                    
                    if (fileHistory?.confidence_insights?.final_confidence_score && fileHistory?.confidence_insights?.final_confidence_score <= 50) {
                        fileHistory.status = 5
                        const insightsObject = JSON.stringify(fileHistory?.confidence_insights, null, 2);
                        const errorDetail = `The confidence score for the file is ${fileHistory?.confidence_insights?.final_confidence_score}, which is below the threshold of 50. The insights object contains the following details: ${insightsObject}. Please review and take necessary actions.`;
                        msg = errorDetail
                        await this.sendErrorMailAndUpdateFileHistory('post_process_validation_failure', errorDetail, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                        return { statusCode, message: msg, error }
                    }
                }
                await downloadFileFromS3IfNotAvailable(outputFile, email);
                let percentageTotalBelow100 = fileHistory?.confidence_insights?.percentage_total_below_threshold || 0;
                let medianFactor = industrTypesMetaData[customerIndustry.industry_type]?.threshold || 100
                fileHistory.status = 6; 

                // We're skipping the validation check if it's an admin or a solar customer  
                if (((percentageTotalBelow100 >= 25) && (percentageTotalBelow100 < 90)) || skipThresholdCheck || (fileHistory?.record_count && fileHistory.record_count < 100)) {
                    statusCode = 200;
                    await fileHistoryRepository.update(fileHistory);
                    await UploadS3(outputFile, createReadStream(outputFile), email)
                    this.downloadResultService(customerRepository, fileHistoryRepository, customerIndustryRepository, integrationsRepository, email, uploadedFileName,);
                    msg = 'Validation done successfully';
                }
                else {
                    fileHistory.status = 5
                    const errorDetail = `percentage of rows below ${medianFactor} is either less than 25 or more than 90, it's at ${percentageTotalBelow100}`;
                    await this.sendErrorMailAndUpdateFileHistory('post_process_validation_failure', errorDetail, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                    if(fileHistory.source ===usageType.LEADGENERATION){
                        updateleadGenerationData(email,findbyemail.name,uploadedFileName,'',{msg:"postprocess_error",details:errorDetail});
                    }
                }
            } else {
                error = 'Invalid file name'
            }
        }
        catch (e) {
            const msg = "error in postProcessingValidationService"
            const error = e.message
            await this.sendErrorMailAndUpdateFileHistory(msg, error, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
        }
        finally {
            this.unLinkFileIfExists(outputFile)
        }
        return { statusCode, message: msg, error }
    }

    static async downloadResultService(customerRepository: any, fileHistoryRepository: FileHistoryRepository, customerIndustryRepository: CustomerIndustryRepository, integrationsRepository: IntegrationsRepository,
       email: string, uploadedFileName: string) {

        const filename_without_extension: string = uploadedFileName.split(".").slice(0, -1).join('.');
        const brandPrefix = checkforCustomBranding(email);
        let finalFile = path.join(__dirname, `../../.sandbox/${filename_without_extension}_rwr_appended_betty.csv`);
        let file = path.join(__dirname, `../../.sandbox/${uploadedFileName}`);
        const processedFileName = `${brandPrefix}_${filename_without_extension}.csv`;
        let outputFile = path.join(__dirname, `../../.sandbox/${processedFileName}`);
        let responseToSend: any = {};
        let fileHistory: FileHistory | null | undefined;
        let findbyemail: Customer | null | undefined;
        let customerIndustry: CustomerIndustry | null | undefined;

        try {
            const findbyemailPromise = await customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: { email },
            });
            const findIndustryPromise = customerIndustryRepository.findOne({
                where: { email },
            });
            const filename: string = uploadedFileName.split(".").slice(0, -1).join('.');
            let fileHistoryPromise = await fileHistoryRepository.findOne({
                where: { and: [{ email }, { filename: uploadedFileName }, { status: 6 }] },
            });
            [fileHistory, findbyemail, customerIndustry] = await Promise.all([fileHistoryPromise, findbyemailPromise, findIndustryPromise]);
            if (fileHistory && findbyemail && customerIndustry) {
                let savingAmountFactor = industrTypesMetaData[customerIndustry.industry_type]?.savings_calculation_factor || 0.67;
                await downloadFileFromS3IfNotAvailable(finalFile, email);
                await downloadFileFromS3IfNotAvailable(file, email);
                let totalRowsBelow100 = fileHistory.rows_below_100 || 0;
                let savingAmount = (totalRowsBelow100 * savingAmountFactor).toFixed(2);
                const scriptPath = path.join(__dirname, '../../python_models/process_betty.py');
                let args = [finalFile, file, brandPrefix]
                let python_output: any = await runPythonScript(scriptPath, args);
                let { output } = await extractPythonResponse({ python_output });

                if (output.success == 'True') {
                    let urlExpiry = 96 * 60 * 60
                    if ('S3_DOWNLOAD_LINK_EXPIRY' in Process.env) {
                        urlExpiry = parseInt(Process.env.S3_DOWNLOAD_LINK_EXPIRY as string)
                    }
                    const downloadUrl = await downloadFileFromS3(path.basename(processedFileName), email, urlExpiry)
                    const options = {
                        savings_amount: savingAmount.toLocaleString(),
                        download_url: downloadUrl
                    }
                    if(fileHistory.source ===usageType.LEADGENERATION){
                        updateleadGenerationData(email,findbyemail.name,uploadedFileName,downloadUrl);
                    }
                    else if (fileHistory?.source == apiUsers.LPG) {
                        await sendMailChimpEmail("dev_done_bettty_without_download_Ver_1", email, filename, findbyemail.name, false, options);
                    }
                    else
                        sendMailChimpEmail("done_betty_00223_ver_1", email, filename, findbyemail.name, false, options);

                    // const run = sendMailChimpEmail("done_betty_00223_ver_1", email, filename, findbyemail.name, false, options);
                    let fileStream = fs.createReadStream(outputFile);

                    const callbackUrl = fileHistory?.meta_data?.callback_url;
                    await UploadS3(outputFile, fileStream, email);
                    if (callbackUrl) {
                        if(fileHistory?.source == platformIntegrations.GOOGLEDRIVE ){
                            uploadFileToGoogleFolder(callbackUrl, processedFileName, email, outputFile);
                        }else 
                            sendFileToCallback(fileHistoryRepository, fileHistory, options);
                    } 
                    if(fileHistory.source == platformIntegrations.HUBSPOT){
                        HubspotService.updateHubspotContacts(integrationsRepository, processedFileName, email);
                    }else if(fileHistory.source == platformIntegrations.ZOHO){
                        ZohoService.updateZohoContacts(integrationsRepository, processedFileName, email);
                    }

                    fileHistory.status = 7;
                    fileHistory.completion_date = new Date();
                    await fileHistoryRepository.update(fileHistory);

                    // dont trigger twit from local
                    if(process.env.NODE_ENV != 'local'){
                        // twit about savings
                        if(savingAmount){
                            await sendTwit(savingAmount)
                            console.log("Twit sent with savings", savingAmount)
                        }
                    }
                    responseToSend.message = 'Success'
                }
                else {
                    await this.sendErrorMailAndUpdateFileHistory('result_error', output.error_details, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
                    if(fileHistory.source ===usageType.LEADGENERATION){
                        updateleadGenerationData(email,findbyemail.name,uploadedFileName,'',{msg:"result_error",details:output.error_details});
                    }
                    return;
                }
            } else {
                responseToSend.error = 'Invalid file name'
            }
        }
        catch (e) {
            responseToSend.error = e.message
            await this.sendErrorMailAndUpdateFileHistory('error in downloadResultService', responseToSend.error, fileHistoryRepository, fileHistory, uploadedFileName, customerRepository, findbyemail)
        }
        finally {
            this.unLinkFileIfExists(outputFile)
            this.unLinkFileIfExists(finalFile)
            this.unLinkFileIfExists(file)
        }
        
        return responseToSend
    }

    static async sendErrorMailAndUpdateFileHistory(errorType: string, error_details: string, fileHistoryRepository: FileHistoryRepository, fileHistory: FileHistory | null | undefined,
        uploadedFileName: string, customerRepository: CustomerRepository, userObj: Customer | null | undefined) {

        if (userObj) {
            const optionsforAdminMail = {
                error: errorType,
                errorDetails: error_details,
                content: null
            }
            await sendEmailToAdmin(uploadedFileName, userObj, customerRepository, optionsforAdminMail);
            if (fileHistory) {
                fileHistory.error = errorType;
                fileHistory.error_detail = error_details;
                await fileHistoryRepository.update(fileHistory);
            }
        }
    }
      
    /**
* Get files and fields for the request
* @param request - Http request
*/
    static async checkFileUploadedService(
        request: Request,
        customerRepository: any,
        fileHistoryRepository: FileHistoryRepository
    ) {
        const findbyemail = await customerRepository.find({
            fields: { login_history: false, file_history: false },
            where: { email: request.headers.email },
        });
        if (findbyemail.length <= 0 || !request.headers.email)
            return { msg: "Email not found", statusCode: 400 };

        if (!request.body.filename)
            return { msg: "Filename not provided", statusCode: 400 };

        if (!request.body.rows)
            return { msg: "Rows not provided", statusCode: 400 };

        //   let previous_files = await fileHistoryRepository.find({where: {email: findbyemail[0].email}, order: ['upload_date DESC']});
        let previous_files = await fileHistoryRepository.find({ where: { email: findbyemail[0].email }, order: ['upload_date DESC'], limit: 2 });

        let foundDuplicateFile = false;

        previous_files.map((item) => {
            const trimmedFilename = item.filename.replace(/\W+/g, '_').replace(/ /g, '_').split("_").slice(0, -2).join('_');
            if (trimmedFilename === request.body.filename && item.record_count.toString() === request.body.rows) {
                foundDuplicateFile = true
            }
        })

        return { data: foundDuplicateFile, statusCode: 200, msg: "File checked successfully" }
    }
}

export type fileHandlerObject = {
    columnMapping: object,
    filename: string
}
