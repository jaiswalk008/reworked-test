
import { downloadCSV } from '../helper';
import path from "path";
import fs from "fs";
import { extractPythonResponse } from "../helper/utils";
import {
    CustomerIndustryRepository, CustomerRepository, FileHistoryRepository,
    AdminEventsRepository, TransactionHistoryRepository,
    IntegrationsRepository,GenerateLeadsRepository } from "../repositories";
import { downloadFileFromS3IfNotAvailable, FileUploadProvider, runPythonScript, UploadS3, generatePresignedS3Url } from "../services";
import { repository } from "@loopback/repository";
import { DbDataSource } from '../datasources';
import { sendEmailToAdmin, sendFileToCallback, getOriginalFileName, parseFileToJSON, checkforCustomBranding } from "../helper"
import { industryTypes } from "../constant/industry_type";
import { platformIntegrations } from '../constant/platform_integrations';
import { downloadFileFromDrive } from '../helper/google-drive';

export class CRMIntegrationService {
    /**
       * Constructor
       * @param customerRepository
       * @param fileHistoryRepository
       * @param handler - Inject an express request handler to deal with the request
       */
    constructor(
        @repository(CustomerRepository)
        public customerRepository: CustomerRepository,
        @repository(FileHistoryRepository)
        public fileHistoryRepository: FileHistoryRepository,
        @repository(CustomerIndustryRepository)
        public customerIndustryRepository: CustomerIndustryRepository,
    ) { }
    /**
     * Get files and fields for the request
     * @param request - Http request
     */

    static async processFile(options: any, customerData: any, fileHistoryObj: any, customerRepository: CustomerRepository,
        fileHistoryRepository: FileHistoryRepository, customerIndustryRepository: CustomerIndustryRepository,
        transactionHistoryRepository: TransactionHistoryRepository, adminEventsRepository: AdminEventsRepository, integrationsRepository: IntegrationsRepository,generateLeadsRepository:GenerateLeadsRepository) {
        const { url, filename } = options;
        const email = customerData.email;
        let downlodaFileFromUrl = true;
        const timestamp = new Date().toISOString().slice(0, -5).replace(/[-:]/g, '');
        let originalFileName: string = '';
        let fileName: string = '';
        let  orgFileName, errorFlag, errorType = null;
        // const fileName = `leadssss.csv`; // Add timestamp to the file name
        // const fileName = `BETTY_leadsss.csv`; // Add timestamp to the file name
        if (url) {
            // file is from google drive and upload on google drive
            if(fileHistoryObj?.source == platformIntegrations.GOOGLEDRIVE && fileHistoryObj?.meta_data?.callback_url){
                function extractFileId(url: string) {
                    const regex = /(?:id=|\/d\/|\/file\/d\/|\/uc\?export=download&id=)([a-zA-Z0-9_-]+)/;
                    const match = url.match(regex);
                    return match ? match[1] : null;
                }
                const fileId = extractFileId(url) || '';
                console.log("fileId", fileId);
                downlodaFileFromUrl= false;
                // ({ orgFileName, errorFlag, errorType } = await downloadFileFromDrive(fileId,"filename"));
                ({ orgFileName, errorFlag, errorType } = await downloadFileFromDrive(fileId));
                fileName = orgFileName;
            }else {
                ({ orgFileName, errorFlag, errorType } = await getOriginalFileName(url));
                originalFileName = orgFileName;
                originalFileName = originalFileName.replace(/\W+/g, '_').replace(/ /g, '_').split("_").slice(0, -1).join('_')
                fileName = `${originalFileName}_${timestamp}.csv`.toLowerCase(); // Add timestamp to the file name
            }

            if (errorFlag) {
                fileHistoryObj.error_detail = errorType;
                fileHistoryObj.error = errorType;
                await fileHistoryRepository.update(fileHistoryObj)
                const optionsforAdminMail = {
                    error: fileHistoryObj.error,
                    errorDetails: fileHistoryObj.error_detail,
                    content: null
                }
                sendEmailToAdmin(fileName, customerData, customerRepository, optionsforAdminMail);
                return
            }
            
        } else {
            originalFileName = filename;
            originalFileName = originalFileName.replace(/\W+/g, '_').replace(/ /g, '_').split("_").slice(0, -1).join('_')
            fileName = filename;
        }

        let newFileUrl = path.join(__dirname, `../../.sandbox/${fileName}`); // Path to and name of object. For example '../myFiles/index.js'.

        try {
            if (url && downlodaFileFromUrl)
                await downloadCSV(url, fileName, email);
            else await downloadFileFromS3IfNotAvailable(newFileUrl, email);
            let newfileStream = fs.createReadStream(newFileUrl);
            // await UploadS3(newFileUrl, newfileStream, email);

            const fileFormat: string = fileName.split(".").pop()!;
            const fileIdGenerate = customerData?.add_ons?.file_id_generate || false;

            let args = ["--file_path", newFileUrl, "--file_format", fileFormat, "--output_path", newFileUrl,"--file_id_generate",fileIdGenerate];
            const scriptPath = path.join(__dirname, '../../python_models/parseUploadFile.py');
            let python_output: any = await runPythonScript(scriptPath, args);
            const { output } = await extractPythonResponse({ python_output });
            fileHistoryObj.filename = fileName;
            fileHistoryObj.file_extension = ".csv";

            if (output.success !== 'True') {
                fileHistoryObj.error_detail = output.error_details;
                fileHistoryObj.error = output.error;
                await fileHistoryRepository.update(fileHistoryObj)
                const optionsforAdminMail = {
                    error: fileHistoryObj.error,
                    errorDetails: fileHistoryObj.error_detail,
                    content: null
                }
                sendEmailToAdmin(fileName, customerData, customerRepository, optionsforAdminMail);
                return
            }
            fileHistoryObj.status = 2;
            // const industryType = industryTypes.REAL_ESTATE_INVESTORS;
            // const findIndustryProfile = await customerIndustryRepository.findOne({ where: { email, industry_type: industryType } });
            const findIndustryProfile = await customerIndustryRepository.findOne({ where: { email } });
            const industryProfile = findIndustryProfile?.industry_profile || []
            // const industrialProfile = industryProfile.find((ele: { question_answers: any; }) => ele.question_answers.industryType == "real_estate_investors")
            const industrialProfile = industryProfile.find((ele: any) => ele.default == true);
            
            // const industrialProfile = industryProfile.find((ele: { name: any; }) => ele.name == 'v1')
            // const stringifiedIndustrialProfile = '{"id":"655e1e790daa910359a2e0b3","question_answers":{"industryType":"real_estate_investors","marketing_campaign":["mail"],"property_type":{"land":["Small< $2,500"]},"title_problems":"dumpster","data_source":"datatree"},"default":false,"name":"v1"}'
            // const industrialProfile = JSON.parse(stringifiedIndustrialProfile)
            let industrialProfileQuestionnaire: any = {};
            if (industrialProfile)
                industrialProfileQuestionnaire = industrialProfile?.question_answers || {};
            else {
                fileHistoryObj.status = 1;
                fileHistoryObj.error_detail = 'Investment profile not found';
                fileHistoryObj.error = 'investmentprofile_not_found';
                await fileHistoryRepository.update(fileHistoryObj)
                return { data: null, msg: fileHistoryObj.error, status: 400 }
            }
            const record_count = (output.mapped_cols as any).row_count;

            fileHistoryObj.record_count = record_count;
            // fileHistoryObj.investment_profile = industrialProfileQuestionnaire;
            fileHistoryObj.industry_profile = industrialProfileQuestionnaire;
            await fileHistoryRepository.update(fileHistoryObj)
            await UploadS3(newFileUrl, newfileStream, email);
            const apiUser = true;
            // FileUploadProvider.postProcessingValidationService(customerRepository, fileHistoryRepository, customerIndustryRepository, integrationsRepository, email, 'hubspot_1729454053871.csv', false);
            FileUploadProvider.fileValidateService(customerRepository, fileHistoryRepository, transactionHistoryRepository, adminEventsRepository, customerIndustryRepository,
                integrationsRepository, generateLeadsRepository,email, fileName, undefined, apiUser);

            return { data: {}, msg: "File submitted for processing", status: 200 }

        } catch (error) {
            fileHistoryObj.status = 1;
            fileHistoryObj.error_detail = error;
            fileHistoryObj.error = 'An error occurred while processing file';
            await fileHistoryRepository.update(fileHistoryObj)
            console.error('An error occurred while processing file:', error.message);
            return { data: null, msg: error.message, status: 400 }
            // Handle the error or log it as needed
        } finally {
            if (fs.existsSync(newFileUrl)) {
                fs.unlinkSync(newFileUrl)
            }
        }

    }

    static async getProcessedFileUrl(email: string, filename: any) {

        // const options = {
        //     download_url: "https://reworked-rei.s3.ap-south-1.amazonaws.com/harshitkyal%40gmail.com/BETTY_leadssss.csv?X-Amz-Algorithm=AWS4-HMAC-SHA256&X-Amz-Credential=AKIA4HJCVYAXLVF43KO6%2F20231105%2Fap-south-1%2Fs3%2Faws4_request&X-Amz-Date=20231105T180440Z&X-Amz-Expires=172800&X-Amz-Signature=b8f98460ca4385828832fc408e0a2b5c70e48bb48c05103de09ad206fcc9d064&X-Amz-SignedHeaders=host"
        // }
        // // send to callback url if added
        // const aa = await sendFileToCallback(fileHistoryObj, options);

        let newFileUrl = path.join(__dirname, `../../.sandbox/${filename}`); // Path to and name of object. For example '../myFiles/index.js'.
        // let newFileUrl = path.join(__dirname, `../../.sandbox/BETTY_leadsss.csv`); // Path to and name of object. For example '../myFiles/index.js'.
        try {
            const expireTime = 2 * 24 * 60 * 60;
            await downloadFileFromS3IfNotAvailable(newFileUrl, email);
            const fileToDownload = generatePresignedS3Url(newFileUrl, email, expireTime)
            return { data: { fileToDownload }, msg: 'File Processed Successfully', success: true }
        } catch (error) {
            console.error('An error occurred:', error);
            return { data: null, msg: error.message, success: false }
        } finally {
            if (fs.existsSync(newFileUrl)) {
                fs.unlinkSync(newFileUrl)
            }
        }

    }

    static formatResponse(errorDetails: string, errorType: string) {

        let statusCode = 500;
        let errorToSend = '';
        let status = 'ERROR';
        if (errorType == 'insufficient_columns') {
            errorToSend = 'Error 103: Requisite columns required to generate a Betty Score is not present. Please contact admin@reworked.ai or call at +1 888 306 1949.';
        }
        else if (errorType == 'no_plan_found') {
            errorToSend = 'Error 101: Please sign up for a plan at reworked.ai before proceeding. You can also contact admin@reworked.ai or call at +1 888 306 1949.'
        }
        else if (errorType == 'investmentprofile_not_found') {
            errorToSend = 'Error 104: Please create an investment profile at reworked.ai before proceeding. You can also contact admin@reworked.ai or call at +1 888 306 1949.'
        }
        else if (errorType == 'stripe_payment_error') {
            errorToSend = 'Error 102: Payment method failed, please update payment method at reworked.ai before proceeding. You can also contact admin@reworked.ai or call at +1 888 306 1949.'
        }
        else if (errorType == 'stripe_payment_method_id_not_found') {
            errorToSend = 'Error 102: Payment method failed, please update payment method at reworked.ai before proceeding. You can also contact admin@reworked.ai or call at +1 888 306 1949.'
        }
        else if( errorType == 'url_access_issue'){
            errorToSend = 'Error 104: File url is not accessible, please try again with correct url. You can also contact admin@reworked.ai or call at +1 888 306 1949.'
        }
        else {
            statusCode = 200;
            status = "PROCESSING"
            errorToSend = 'File is already in process, pls check after sometime'
        }
        console.log("Api respnse before format", { errorDetails, errorType })
        console.log("Api respnse  after format", { statusCode, formatResponse: errorToSend, status })
        return { statusCode, formatResponse: errorToSend, status };

    }

    static async getProcessedData(fileHistoryData: any, email: string) {
        let responseObjToSend: any = {};
        const fileUUID = fileHistoryData.id;
        let responseObj = null;

        let fileName = `BETTY_${fileHistoryData.filename}`;
        const brandPrefix = checkforCustomBranding(email);
        fileName = fileName.replace("BETTY", brandPrefix);

        if (fileHistoryData.input_type == 'JSON') {
            responseObj = await parseFileToJSON(fileName, fileHistoryData.email)
            responseObjToSend = {
                parsed_output: responseObj?.data?.parsedData
            }
        }
        else {
            responseObj = await this.getProcessedFileUrl(email, fileName);
            responseObjToSend = {
                processed_file_url: responseObj?.data?.fileToDownload
            }
        }

        if (responseObj.success)
            return { msg: responseObj.msg, data: { file_upload_identifier: fileUUID, ...responseObjToSend, status: "PROCESSED", statusCode: 200 } };
        else {
            let { statusCode, formatResponse, status } = this.formatResponse(fileHistoryData.error_detail || '', fileHistoryData.error || '')
            return { msg: formatResponse, data: { error_detail: fileHistoryData, file_upload_identifier: fileUUID, processed_file_url: null, status, statusCode } };
        }
    }
}