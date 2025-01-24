
import {
    Request,
} from "@loopback/rest";
import {
    BindingScope,
    ContextTags,
    injectable,
} from '@loopback/core';
// import fs from "fs";
import { FILE_UPLOAD_SERVICE } from '../keys';
import { CustomerModelsRepository, GenerateLeadsRepository, TransactionHistoryRepository, AdminEventsRepository, CustomerRepository, FileHistoryRepository, CustomerIndustryRepository, IntegrationsRepository } from "../repositories";
import { ageRange, householdIncomeRange } from "../constant/generate_leads";
import axios from 'axios';
import { MelissaListApiOptions } from '../interface/Melissa/melissa-list-api-options.interface'
import { cleanCSVFile, pollCSVAvailability, sendEmailToAdmin } from '../helper';
import { GenerateLeadArgs } from '../interface/generate-leads.interface';
import { QueryParams } from '../interface/Melissa/milessa-lead-query-params.interface';
import { stripePayment, stripPaymentInfo } from '../helper/stripe-payment';
import { DbDataSource } from '../datasources';
import * as fs from 'fs/promises';
import fss from "fs";
// import FS from 'fs';
import { GenerateLeadsModel, Customer, FileHistory } from "../models";
import stripeClient from "../services/stripeClient";
import { getFilterSort } from "../helper/filter-sort";
import path from "path";
import { parse } from 'csv-parse';
import { createObjectCsvWriter as createCsvWriter } from 'csv-writer';
import { CRMIntegrationService } from "./crm-integration.service";
import { usageType } from "../constant/usage_type";
import { industrTypesMetaData, industryTypes } from "../constant/industry_type";
import { leadgenModel } from "../constant/lead_gen_model";
import { UploadS3 } from "./awsServices";

@injectable({
    scope: BindingScope.TRANSIENT,
    tags: { [ContextTags.KEY]: FILE_UPLOAD_SERVICE },
})
// Melissa api call
// https://list.melissadata.net/v1/Consumer/rest/Service.svc/get/zip?id=120506494&zip=75023,75024,75025,75013,75014,75002,75069,75070&qty=1000&cAge-d=5-6-7&hInc=7-8-9-10-11-12-13&marital=1&adults-d=2&kids=1
export class GenerateLeadsService {

    constructor() { }

    private static calculateMaxZipCodesPerRequest(leadCount: number): number {
        if (leadCount > 1000) {
            return 10; // If lead count is greater than 1000, take 10 zip codes per request
        } else if (leadCount > 100) {
            return 5; // If lead count is greater than 100, take 5 zip codes per request
        } else {
            return 1; // Otherwise, take 1 zip code per request
        }
    }

    /**
     * Get files and fields for the request
     * @param request - Http request
     */

    static async generateLeads(generateLeadArgs: GenerateLeadArgs, customerModelData: any, generateLeadsRepository: GenerateLeadsRepository,
        customerRepository: CustomerRepository, adminEventsRepository: AdminEventsRepository, transactionHistoryRepository: TransactionHistoryRepository,
        generateLeadsModelData: GenerateLeadsModel, customerData: Customer, fileHistoryRepository: FileHistoryRepository, customerIndustryRepository: CustomerIndustryRepository, integrationsRepository: IntegrationsRepository) {
        // const dataSource = new DbDataSource(); // Replace with your actual data source instantiation
        const { leadsCount, email, totalCost, defaultModel, zipCodes } = generateLeadArgs;
        // const customerRepository = new CustomerRepository(dataSource);
        // const transactionHistoryRepository = new TransactionHistoryRepository(dataSource);
        // const adminEventsRepository = new AdminEventsRepository(dataSource);

        try {
            const customerIndustryDetails = await customerIndustryRepository.findOne({ where: { email: customerData.email } })
            const industryType = customerIndustryDetails?.industry_type || 'default';

            let { data, msg, statusCode } = { data: {}, msg: '', statusCode: 200 };
            let response;
            if (totalCost) {
                const sourceType = 'lead_generation';
                const metaData = {
                    "no_of_credits": leadsCount,
                    "total_cost": totalCost,
                    "source_type": sourceType,
                    "leads_to_add": false,
                }

                const args = {
                    totalAmount: totalCost, metaData, noOfCredits: leadsCount, email, payment_type: "Lead Generation",
                    invoiceItemDescription: "Buy leads", sourceType, leadsToAdd: false,
                };

                response = await stripePayment(args, customerData, customerRepository, transactionHistoryRepository
                    , adminEventsRepository)
            }
            if ((process.env.NODE_ENV == 'development' || process.env.NODE_ENV == 'local') && process.env.NODE_ENV) {
                generateLeadArgs.leadsCount = 1;
            }
            if (response) {
                data = response?.data;
                msg = response?.msg;
                statusCode = response?.statusCode;
            }
            try {
                let last4Digit = await stripPaymentInfo(customerData);
                data = { ...data, cc_last4digit: last4Digit?.cardInfo?.last4, brand: last4Digit?.cardInfo?.brand }
            } catch (error) {
                console.error("Error in stripPaymentInfo function", error)
            }
            if (statusCode == 200) {
                // if default model then hard code sequences
                // if (defaultModel) {
                //     const defaultLeadGenModel = leadgenModel?.[industryType]
                //     customerModelData = {
                //         // criteria: {
                //         //     melissaAgeSequence: defaultLeadGenModel?.criteria?.['cAge-d'],
                //         //     melissaHouseholdSequence: defaultLeadGenModel?.criteria?.hInc
                //         // }
                //     }
                // }
                this.getZipCodesToBuyLeads(generateLeadArgs, customerModelData, generateLeadsModelData, generateLeadsRepository, customerData, fileHistoryRepository, customerRepository, customerIndustryRepository, transactionHistoryRepository, adminEventsRepository, integrationsRepository).then(async (ele: any) => {
                    let { data, success, errorMsg, errorDetail } = ele;
                    const options = data?.options;
                    if (success) {
                        options.callType = 'buy'
                        generateLeadsModelData.leads_api_options = options;
                        const statusFromMelissa = await this.processMelissaCSV(generateLeadsModelData, generateLeadsRepository, customerData, fileHistoryRepository, customerRepository, customerIndustryRepository, transactionHistoryRepository, adminEventsRepository, integrationsRepository);
                        success = statusFromMelissa?.success;
                        generateLeadsModelData.error = statusFromMelissa?.error;
                        generateLeadsModelData.error_detail = statusFromMelissa?.error_detail;
                    } else {
                        success = false;
                        generateLeadsModelData.error = errorMsg;
                        generateLeadsModelData.error_detail = errorDetail || errorMsg;
                    }
                    if (!success) {
                        const optionsforAdminMail = {
                            error: generateLeadsModelData.error,
                            errorDetails: generateLeadsModelData.error_detail,
                            content: `Lead Generation failed of customer : ${customerData?.name} with email : ${customerData?.email} has failed due to ${generateLeadsModelData.error} with error detail ( ${generateLeadsModelData.error_detail} ), pls check and review.`
                        }
                        await generateLeadsRepository.update(generateLeadsModelData);
                        // send email to admin
                        sendEmailToAdmin('filename', customerData, customerRepository, optionsforAdminMail)
                    }
                });
                msg = 'Leads request submitted successfully, we will notify you as soon as it completes.';
            } else {
                generateLeadsModelData.error = "error while doing payment";
                generateLeadsModelData.error_detail = msg;
                const optionsforAdminMail = {
                    error: generateLeadsModelData.error,
                    errorDetails: generateLeadsModelData.error_detail,
                    content: `Lead Generation failed of customer : ${customerData?.name} with email : ${customerData?.email} has failed due to ${generateLeadsModelData.error} with error detail ( ${generateLeadsModelData.error_detail} ), pls check and review.`
                }
                await generateLeadsRepository.update(generateLeadsModelData);
                // send email to admin
                sendEmailToAdmin('filename', customerData, customerRepository, optionsforAdminMail)

            }
            return { data, msg, status: statusCode }
        } catch (error) {
            console.error('An error occurred:', error);
            generateLeadsModelData.error = "error in generate leads function";
            generateLeadsModelData.error_detail = error.message;
            const optionsforAdminMail = {
                error: generateLeadsModelData.error,
                errorDetails: generateLeadsModelData.error_detail,
                content: `Lead Generation failed of customer : ${customerData?.name} with email : ${customerData?.email} has failed due to ${generateLeadsModelData.error} with error detail ( ${generateLeadsModelData.error_detail} ), pls check and review.`
            }
            await generateLeadsRepository.update(generateLeadsModelData);
            // send email to admin
            sendEmailToAdmin('filename', customerData, customerRepository, optionsforAdminMail)
            return { data: null, msg: error.message, status: 400 }
            // Handle the error or log it as needed
        }

    }

    static async leadsAvailability(generateLeadArgs: GenerateLeadArgs, customerModelData: any, industryType: string) {
        try {
            const { leadsCount, placeList, zipCodes, defaultModel = false } = generateLeadArgs;
            let searchByZip = zipCodes.length > 0 ? true : false;

            let customModelCriteria = {};
            if ((!defaultModel && !customerModelData) || (!placeList?.length && !zipCodes?.length)) {
                return { success: false, data: null, errorMsg: "Place list is empty" }
            }
            let searchType = 'state';
            let placeParameter = [];
            let placeField = 'st';
            let totalLeads = 0;

            const {  zipcodes: countyZipCode, stateIds, countyIds } = await this.processPlaceList(placeList)
            placeParameter = stateIds;

            if (!defaultModel) {
                customModelCriteria = {
                    'cAge-d': customerModelData.criteria?.melissaAgeSequence,
                    hInc: customerModelData.criteria?.melissaHouseholdSequence,
                }
            }
            const options: {
                callType: string;
                searchType: string;
                defaultModel: boolean
                queryParams: QueryParams;
            } = {
                callType: 'get',
                searchType,
                defaultModel,
                queryParams: {
                    qty: leadsCount,
                    ...customModelCriteria
                }
            };

            if (searchByZip || countyZipCode?.length) {
                searchType = 'zip';
                placeField = 'zip';
                let finalZipCodes = zipCodes.length ? zipCodes : countyZipCode; // Fallback to countyZipCode if zipCodes is empty
                // If total zip codes exceed 30, randomly select 30 zip codes
                if (finalZipCodes.length > 40) {
                    finalZipCodes = finalZipCodes.sort(() => Math.random() - 0.5).slice(0, 30);
                }
                (options.queryParams as any)[`${placeField}`] = finalZipCodes.join(',');
                // (options.queryParams as any)[`${placeField}`] = zipCodes.join(',');
            }
            else if (countyIds && countyIds.length) {
                searchType = 'county';
                placeField = 'county';
                placeParameter = countyIds;
                if (!placeParameter || placeParameter.length === 0) {
                    return { success: false, data: null, errorMsg: `Something is wrong with region selected: ${placeList}` }
                }
                (options.queryParams as any)[`${placeField}`] = placeParameter.join(',');
            }
            options.searchType = searchType;
            const melissaResponse = await GenerateLeadsService.melissaListApi(options, industryType);
            const leadsInResponse = melissaResponse?.data?.data?.summary?.totalCount || 0;
            totalLeads += parseInt(leadsInResponse);
            console.log(`${totalLeads} leads available in ${placeList} and requested leads are ${leadsCount}`);

            const data = { options, totalFetchedLeads: totalLeads, requestedLeads: leadsCount };
            if (totalLeads >= leadsCount) {
                return { success: true, data: { ...data, leadsAvailable: true } }
            } else return { success: false, data: null, errorMsg: `Requested leads are not available`, errorDetail: `Request leads are ${leadsCount} and available leads are ${totalLeads} with options ${JSON.stringify(options)}` }

        } catch (error) {
            console.error('An error occurred:', error);
            return { success: false, data: null, errorMsg: error.message, errorDetail: JSON.stringify(error) }
            // Handle the error or log it as needed
        }
    }

    static async getZipCodesToBuyLeads(generateLeadArgs: GenerateLeadArgs, customerModelData: any, generateLeadsModelData: any, generateLeadsRepository: GenerateLeadsRepository, customerData: any, fileHistoryRepository: FileHistoryRepository,
        customerRepository: CustomerRepository, customerIndustryRepository: CustomerIndustryRepository, transactionHistoryRepository: TransactionHistoryRepository, adminEventsRepository: AdminEventsRepository, integrationsRepository: IntegrationsRepository) {
        try {
            let errorMsg = null;
            let customModelCriteria = {};
            const { placeList, leadsCount, nationWide, zipCodes, defaultModel = false } = generateLeadArgs;
            const customerIndustryDetails = await customerIndustryRepository.findOne({ where: { email: customerData.email } })
            const industryType = customerIndustryDetails?.industry_type || ''

            if (!nationWide && (placeList.length > 0 || zipCodes?.length)) return await this.leadsAvailability(generateLeadArgs, customerModelData, industryType)
            if ((!defaultModel && !customerModelData) || !leadsCount) {
                return { success: false, data: null };
            }
            let searchType = 'zip';
            let placeField = 'zip';

            let zipcodeToCallMelissa: any = [];

            const sortedZipCodeList = customerModelData.zipcode_sorted_list || [];

            // Function to pad zip codes to 5 digits
            const padZipCode = (zipCode: string): string => {
                return zipCode.padStart(5, '0');
            }
            const paddedZipcodes = customerModelData.zipcode_sorted_list?.map((zipCode: string) => padZipCode(zipCode));

            if (!(paddedZipcodes && paddedZipcodes.length)) return { success: false, data: null, errorMsg: 'Zipcodes are not available' }
            const processedZipCodesProbabilities: Record<string, number> = {};

            sortedZipCodeList.forEach((item: any) => {
                const zipCode = Object.keys(item)[0];
                const probability = parseFloat(item[zipCode]);
                processedZipCodesProbabilities[padZipCode(zipCode)] = probability;
            });
            // Sort the zip codes based on probabilities in descending order
            const sortedZipCodes = paddedZipcodes.sort((a: string | number, b: string | number) => {
                const probabilityA = processedZipCodesProbabilities[a];
                const probabilityB = processedZipCodesProbabilities[b];

                return probabilityB - probabilityA; // Sort in descending order of probability
            });
            if (!defaultModel) {
                customModelCriteria = {
                    'cAge-d': customerModelData.criteria?.melissaAgeSequence,
                    hInc: customerModelData.criteria?.melissaHouseholdSequence,
                }
            }
            const maxZipCodesPerRequest = this.calculateMaxZipCodesPerRequest(leadsCount); // Calculate the max zip codes based on lead count
            const options: {
                callType: string;
                searchType: string;
                defaultModel: boolean;
                queryParams: QueryParams;
            } = {
                callType: 'buy',
                searchType,
                defaultModel,
                queryParams: {
                    qty: leadsCount,
                    ...customModelCriteria
                }
            };
            let melissaResponse: any;
            let totalLeads = 0;
            let zipcodesUsed = 0;
            while (totalLeads < leadsCount) {
                if (sortedZipCodeList.length === 0) {
                    errorMsg = 'Insufficient zip codes for the desired lead count.';
                    break;
                }
                const zipCodesToRequest = sortedZipCodes.splice(0, maxZipCodesPerRequest); // Take multiple zip codes at once
                zipcodesUsed += zipCodesToRequest.length;
                zipCodesToRequest.map((zipCodeObj: any) => {
                    const zipCode = zipCodeObj;
                    const formattedZipcode = String(zipCode).padStart(5, '0');
                    zipcodeToCallMelissa.push(formattedZipcode);
                    return formattedZipcode;
                });

                // options.queryParams.zip = zipcodeToCallMelissa.join(',');
                (options.queryParams as any)[`${placeField}`] = zipcodeToCallMelissa.join(',');
                if (options.queryParams.zip) {
                    melissaResponse = await GenerateLeadsService.melissaListApi(options, industryType);
                    const leadsInResponse = melissaResponse?.data?.data?.Consumer?.TotalCount?.Count || 0;
                    totalLeads += parseInt(leadsInResponse);
                    if (totalLeads === 0) {
                        errorMsg = 'Insufficient zip codes for the desired lead count.';
                        break;
                    }
                    console.log(`Leads so far: ${totalLeads}`);
                } else break;
                //if all zipcodes got used, then break the loop otherwise it will keep on calling melissa api
                if (zipcodesUsed > paddedZipcodes.length) {
                    break;
                }
            }
            console.log(`${totalLeads} leads available in region selected.`);
            if (totalLeads >= leadsCount) {
                return { success: true, data: { options, totalFetchedLeads: totalLeads } }
            } else return { success: false, data: null, errorMsg }

        } catch (error) {
            console.error('An error occurred:', error);
            return { success: false, data: null, errorMsg: error.message }
            // Handle the error or log it as needed
        }

    }

    static async getGeneratedListHistory(
        email: string,
        customerRepository: any,
        generateLeadsRepository: GenerateLeadsRepository,
        modelName: string,
        idAdmin: boolean,
        defaultModel: boolean
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


        if (defaultModel) {
            whereObj['default_model'] = true;
        } else if (modelName) {
            whereObj['model_name'] = modelName;
        }
        let previous_files = await generateLeadsRepository.find({
            fields: ['email', 'updated_at', 'created_at', 'status', 'amount_spent', 'id', 'model_name', 'rwr_list_url',
                'lead_count', 'file_name', 'status', 'rwr_count', 'email', 'place_list', 'error', 'error_detail'],
            where: whereObj,
            order: ['updated_at DESC']
        });

        return previous_files
    }

    static async getAllGeneratedListHistory(
        request: Request,
        generateLeadsRepository: GenerateLeadsRepository,
    ) {
        // const whereObj: any = {};
        let where: any = {}
        let order: string = 'upload_date DESC'


        const searchEmail: any = request.query.email;
        const sortName: any = request.query.sortName;
        const type: any = request.query.type;
        const page: any = request.query.page || 1;

        if (searchEmail) {
            where['email'] = searchEmail
        }

        let filtertype = "leadgeneration"
        const filterSort = getFilterSort({ where, filtertype, sortName, type })

        where = filterSort.where
        if (filterSort.order !== '') {
            order = filterSort.order
        }

        const limit = 10;
        const offset = (page - 1) * limit;

        const totalCountPromise = await generateLeadsRepository.count(where);

        // let previous_files = await customerModelsRepository.find({
        //     fields: ['email', 'name', 'error', 'error_detail', 'updated_at', 'created_at', 'vendor_list_url', 'description', 'status', 'id', 'insights'],
        //  where: where, order: [order], limit: limit, skip: offset });

        let previous_files = await generateLeadsRepository.find({
            fields: ['email', 'updated_at', 'created_at', 'status', 'amount_spent', 'id', 'model_name', 'rwr_list_url', 'leads_provider_file',
                'lead_count', 'file_name', 'status', 'rwr_count', 'email', 'place_list', 'error', 'error_detail'],
            where: where,
            order: [order],
            limit: limit,
            skip: offset
        });

        const [totalCount, data] = await Promise.all([totalCountPromise, previous_files]);
        const length = totalCount?.count

        // return previous_files
        return {
            length: length,
            data: data
        };



        // let previous_files = await generateLeadsRepository.find({
        //     fields: ['email', 'updated_at', 'created_at', 'status', 'amount_spent', 'id', 'model_name', 'rwr_list_url', 'leads_provider_file',
        //         'lead_count', 'file_name', 'status', 'rwr_count', 'email', 'place_list', 'error', 'error_detail'],
        //     where: where,
        //     order: ['updated_at DESC'],
        //     limit: 10,
        // });

        // return previous_files
    }

    static findMelissaSequences(inputRange: number[], sourceType: string) {

        try {
            if (!(inputRange && inputRange.length)) {
                console.error("Input range is null.");
                return null;
            }
            let resourceObj: any = {};
            if (sourceType == 'age') {
                resourceObj = ageRange

                const sequences: any = [];
                for (const sequence in resourceObj) {
                    const range = resourceObj[sequence];
                    if (inputRange[0] <= range[1] && inputRange[1] >= range[0]) {
                        sequences.push(parseInt(sequence));
                    };
                }

                sequences.sort((a: any, b: any) => a - b);
                const sequenceArray = Array.from({ length: sequences[sequences.length - 1] - sequences[0] + 1 }, (_, index) => sequences[0] + index);
                return sequenceArray && sequenceArray.length ? sequenceArray.join("-") : null;
            }
            else {
                // const ranges = inputRange..replace(/[\[\]"']/g, '').split(',');
                let result = inputRange.map((range: any) => {
                    const [rawStart, rawEnd] = range.replace('k', '000').split('-');
                    const start = rawStart.includes('k') ? parseInt(rawStart, 10) * 1000 : parseInt(rawStart, 10);
                    const end = rawEnd ? (rawEnd.includes('k') ? parseInt(rawEnd, 10) * 1000 : parseInt(rawEnd, 10)) : 1000000000;
                    return [start, end];
                });
                resourceObj = householdIncomeRange
                const keysForRanges: any = [];
                const rangeOutputSet = new Set(); // Use a Set to ensure uniqueness
                // result = [[53000, 74000]]
                for (const rangee of result) {
                    let rangeOutputArray = [];
                    for (const sequence in resourceObj) {
                        const range = resourceObj[sequence];
                        if (rangee[0] <= range[1] && rangee[1] >= range[0]) {
                            rangeOutputArray.push(parseInt(sequence));
                            rangeOutputSet.add(parseInt(sequence));
                        };
                    }
                    keysForRanges.push(rangeOutputArray);
                };
                const mergedKeysForRanges = (Array.from(rangeOutputSet) as number[])
                    .sort((a, b) => a - b)
                    .map(String)
                    .join('-');
                console.log("keysForRanges", mergedKeysForRanges)
                return mergedKeysForRanges;
                // inputRange = []
            }

        } catch (error) {
            console.error("error in findMelissaSequences", error);
            return null;
        }
    }

    private static buildUrl(baseUrl: string, queryParams: object) {
        const url = new URL(baseUrl);

        for (const [key, value] of Object.entries(queryParams)) {
            url.searchParams.append(key, value);
        }
        return url.toString();
    }

    static async melissaListApi(options: MelissaListApiOptions, industryType: string = '', modelCreation: boolean = false) {

        try {
            let { callType, searchType, queryParams, defaultModel = false } = options;
            if (callType == 'get') callType = 'Count';
            if (!industryType) industryType = "default"
            const defaultLeadGenModel = leadgenModel?.[industryType]
            // const baseUrl = `${defaultLeadGenModel.api_url}/${callType}/Json/${searchType}`;
            const baseUrl = `${defaultLeadGenModel.api_url}/${callType}_${searchType}`;
            leadgenModel
            let queryParamsObj = {}
            if (modelCreation || !defaultModel) {
                queryParamsObj = {
                    ...queryParams,
                    id: defaultLeadGenModel.api_key,
                }
            } else {
                queryParamsObj = {
                    ...queryParams,
                    id: defaultLeadGenModel.api_key,
                    ...defaultLeadGenModel.criteria
                }
            }
            // const melissalistResponse = await axios.get(baseUrl, { params: queryParamsObj });
            const apiUrl = this.buildUrl(baseUrl, queryParamsObj);
            console.log("apiUrl", apiUrl)
            const melissalistResponse = await axios.get(apiUrl);
            // const totalCount = melissalistResponse?.data?.summary?.totalCount || 0;
            return { data: melissalistResponse, error: false, msg: "Melissa api called successfully" }

        } catch (error) {
            console.error("error in MelissaListApi", error);
            return { data: null, error: true, msg: error.message }
        }
    }

    static async processMelissaCSV(generateLeadsModelData: any, generateLeadsRepository: GenerateLeadsRepository, customerData: any, fileHistoryRepository: FileHistoryRepository,
        customerRepository: CustomerRepository, customerIndustryRepository: CustomerIndustryRepository, transactionHistoryRepository: TransactionHistoryRepository, adminEventsRepository: AdminEventsRepository,
        integrationsRepository: IntegrationsRepository, retryFlag: boolean = false) {
        // if retry flag is true then default retry limit is 5
        let retryLimit = 10;
        const customerIndustryDetails = await customerIndustryRepository.findOne({ where: { email: customerData.email } })
        const industryType = customerIndustryDetails?.industry_type
        try {
            const options = generateLeadsModelData.leads_api_options;

            let leadsFile = null;
            // if retry flag is true then will not call melissa instead once will try with existing melissa url
            if (!retryFlag) {
                const melissaResponse = await GenerateLeadsService.melissaListApi(options, industryType);
                // leadsFile = melissaResponse?.data?.data?.Consumer?.Order?.DownloadURL || null;
                leadsFile = melissaResponse?.data?.data?.summary?.fileUrl || null;
                // leadsFile = "https://list.melissadata.com/ListOrderFiles/7125253_1205064941.csv";
                if (!leadsFile) {
                    return {
                        success: false,
                        error: "Error while getting leads",
                        error_detail: "No leads File url from Melissa"
                    };
                }

                const timestamp = new Date().toISOString().replace(/[-:.]/g, '');
                generateLeadsModelData.file_name = `leads_${timestamp}.csv`
                generateLeadsModelData.leads_provider_file = leadsFile;
                // generateLeadsModelData.rwr_count = parseInt(melissaResponse?.data?.data?.Consumer?.TotalCount?.Count || 0);
                generateLeadsModelData.rwr_count = parseInt(melissaResponse?.data?.data?.summary?.totalCount || 0);
                // generateLeadsModelData.rwr_count = 1;

                await generateLeadsRepository.update(generateLeadsModelData);
            } else {
                retryLimit = 0;
                leadsFile = generateLeadsModelData.leads_provider_file;
            }

            if (industryType === industryTypes.SOLAR_INSTALLER || industryType === industryTypes.ROOFING) {
                this.processMelissaCSVFile(
                    leadsFile,
                    generateLeadsModelData,
                    customerData.email,
                    customerData,
                    fileHistoryRepository, customerRepository,
                    customerIndustryRepository,
                    transactionHistoryRepository,
                    adminEventsRepository,
                    integrationsRepository,
                    generateLeadsRepository,
                    retryLimit
                )
            }
            else {
                pollCSVAvailability({
                    data: {
                        csvUrl: leadsFile,
                        generateLeadsRepository,
                        retryLimit,
                        objectIdToUpdate: generateLeadsModelData.id,
                        customerName: customerData?.name,
                        customerEmail: customerData?.email,
                        retryFlag
                    }
                }).then((obj) => {
                    console.log("error", obj)
                    // if retry flag is true and success is false then restart melissa process to get url because url provided by melissa is having some issue
                    if (retryFlag && !obj?.success) {
                        this.processMelissaCSV(generateLeadsModelData, generateLeadsRepository, customerData, fileHistoryRepository, customerRepository, customerIndustryRepository, transactionHistoryRepository, adminEventsRepository, integrationsRepository);
                    }
                });
            }

            return {
                success: true,
                error: "",
                error_detail: ""
            };
        } catch (error) {
            return {
                success: false,
                error: "Error while getting leads",
                error_detail: error.message
            };
        }
    }

    static async processMelissaCSVFile(
        csvUrl: string,
        generateLeadsModelData: any,
        email: string,
        customerData: Customer,
        fileHistoryRepository: FileHistoryRepository,
        customerRepository: CustomerRepository,
        customerIndustryRepository: CustomerIndustryRepository,
        transactionHistoryRepository: TransactionHistoryRepository,
        adminEventsRepository: AdminEventsRepository,
        integrationsRepository: IntegrationsRepository,
        generateLeadsRepository: GenerateLeadsRepository, retryLimit: number
    ): Promise<void> {
        try {

            const filename = generateLeadsModelData.file_name;
            let downloadedLeadGenFilePath = path.join(__dirname, `../../.sandbox/${filename}`); // Path to and name of object. For example '../myFiles/index.js'.

            // Function to fetch the CSV file
            const fetchCSVFile = async (): Promise<boolean> => {
                try {
                    const response = await axios.get(csvUrl, { responseType: 'stream' });
                    const writeStream = fss.createWriteStream(downloadedLeadGenFilePath);

                    await new Promise<void>((resolve, reject) => {
                        response.data.pipe(writeStream);
                        writeStream.on('finish', resolve);
                        writeStream.on('error', reject);
                    });

                    console.log('File downloaded successfully.');
                    return true;
                } catch (error) {
                    console.log('File not ready yet.');
                    return false;
                }
            };
            let attempts = 0;

            while (attempts < retryLimit) {
                const isFileReady = await fetchCSVFile();
                if (isFileReady) break;

                attempts++;
                if (attempts < retryLimit) {
                    console.log(`Retrying in 60 seconds... (${attempts}/${retryLimit})`);
                    await new Promise((resolve) => setTimeout(resolve, 60000));
                }
            }

            if (attempts === retryLimit) {
                throw new Error('File not ready after maximum attempts.');
            }

            await cleanCSVFile(downloadedLeadGenFilePath);
            let newfileStream = fss.createReadStream(downloadedLeadGenFilePath);
            await UploadS3(downloadedLeadGenFilePath, newfileStream, email);
            if (fss.existsSync(downloadedLeadGenFilePath)) {
                fss.unlinkSync(downloadedLeadGenFilePath);
            }
            let fileHistoryObj = new FileHistory({
                email,
                filename: filename,
                file_extension: 'csv',
                upload_date: new Date(),
                record_count: generateLeadsModelData.lead_count,
                status: 2,
                source: usageType.LEADGENERATION
            });
            const savedFileHistory = await fileHistoryRepository.create(fileHistoryObj);
            const options = { filename };
            await CRMIntegrationService.processFile(
                options,
                customerData,
                savedFileHistory,
                customerRepository, fileHistoryRepository,
                customerIndustryRepository,
                transactionHistoryRepository,
                adminEventsRepository,
                integrationsRepository,
                generateLeadsRepository
            );

        } catch (error) {
            console.error('Error processing Melissa CSV:', (error as Error).message);
        }
    }


    private static processPlaceList = async (placeList: string[]) => {
        const filePath = 'src/data/county_state__zipcode_json.json';
        // Read the JSON file and store its contents in a variable
        const data = await fs.readFile(filePath, 'utf8');
        // Parse the JSON data into a JavaScript object
        const stateCountyZipcodeObject = JSON.parse(data);
        let zipcodes: string[] = [];
        let stateIds: string[] = [];
        let countyIds: string[] = [];

        let zipCount = placeList.length > 2 ? 10 : 20;  // Take 10 if more than 3 zip codes, otherwise 20

        for (const place of placeList) {
            const placeName: any = place;
            if (stateCountyZipcodeObject[placeName]) {
                const obj = stateCountyZipcodeObject[placeName];
                const randomZipcodes = obj?.zipcode
                                        .sort(() => Math.random() - 0.5)
                                        .slice(0, Math.min(obj?.zipcode.length, zipCount));
                zipcodes = zipcodes.concat(randomZipcodes);
                stateIds.push(obj.state_id);
                if (placeName.includes('county')) {
                    countyIds.push(`${obj.state_id};${placeName.split(',')[0]}`);
                }
            }
        }
        return { zipcodes, stateIds, countyIds }
    }


}
