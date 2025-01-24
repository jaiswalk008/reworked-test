// Uncomment these imports to begin using these cool features!

import { inject } from "@loopback/core";
import {
    get,
    post,
    Request,
    requestBody,
    Response,
    RestBindings,
} from "@loopback/rest";
import {
    CustomerModelsRepository, CustomerRepository, FileHistoryRepository, GenerateLeadsRepository, CustomerIndustryRepository, ReferralRepository,
    TransactionHistoryRepository, AdminEventsRepository, IntegrationsRepository
} from "../repositories";
import { TokenServiceBindings } from '@loopback/authentication-jwt';
import { authenticate, TokenService } from '@loopback/authentication';
import { repository } from "@loopback/repository";
import { CRMIntegrationService } from "../services/crm-integration.service";
import { FileHistory, Customer } from "../models";
import { generateApiKey, getOauthToken, checkOauthUser, createHash, parseFileToJSON, convertJsonToCsv, calculateRowsLeftForUser } from "../helper";
import { UploadS3 } from "../services";
import path from "path";
import fs from "fs";
const baseUrl = '/api'
const jwt = require('jsonwebtoken');
import { apiUsers } from '../constant/api_users';
import { stripPaymentInfo } from "../helper/stripe-payment";
import { industryTypes } from "../constant/industry_type";
// Your secret key for signing the token (keep this secret)
const crmSecretKey = process.env.CRM_API_KEY;
export class CRMIntegrationController {
    /**
     * Constructor
     * @param customerRepository
     * @param fileHistoryRepository
     * * @param GenerateLeadsRepository
     * @param handler - Inject an express request handler to deal with the request
     */
    constructor(
        @inject(TokenServiceBindings.TOKEN_SERVICE)
        public jwtService: TokenService,
        @repository(GenerateLeadsRepository)
        public generateLeadsRepository: GenerateLeadsRepository,
        @repository(CustomerModelsRepository)
        public customerModelsRepository: CustomerModelsRepository,
        @repository(CustomerRepository)
        public customerRepository: CustomerRepository,
        @repository(FileHistoryRepository)
        public fileHistoryRepository: FileHistoryRepository,
        @repository(CustomerIndustryRepository)
        public customerIndustryRepository: CustomerIndustryRepository,

        @repository(TransactionHistoryRepository)
        public transactionHistoryRepository: TransactionHistoryRepository,
        @repository(AdminEventsRepository)
        public adminEventsRepository: AdminEventsRepository,
        @repository(IntegrationsRepository)
        public integrationsRepository: IntegrationsRepository,
        

        @repository(ReferralRepository)
        public referralRepository: ReferralRepository,
        


    ) { }


    // @authenticate('jwt')
    @post(`${baseUrl}/generate-token`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Process leads',
            },
        },
    })
    async generateToken(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: ["api_key", "email"],
                        properties: {
                            api_key: {
                                type: "string",
                            },
                            email: {
                                type: "string",
                            },
                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        try {
            const requestBody = request?.body;
            const crmKey = requestBody?.api_key;
            const email = requestBody?.email;
            const customerModelData = await this.customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: {
                    email,
                }
            })

            if (customerModelData) {
                if (customerModelData.api_secret_key === crmKey) {
                    const payload = {
                        crmKey,
                        email
                    };
                    const token = jwt.sign(payload, crmSecretKey, { expiresIn: '48h' });
                    return response.status(200).send({ msg: 'Token generated successfully', data: { token } });
                } else {
                    return response.status(404).send({ msg: 'Api key is invalid', data: null });
                }
            } else {
                return response.status(404).send({ msg: 'Customer data not found', data: null });
            }

        } catch (error) {
            console.log(error);
            return response.status(500).send({ msg: error.message, data: null });
        }
    }

    // @authenticate('jwt')
    @post(`${baseUrl}/process-leads`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Process leads',
            },
        },
    })
    async processLeads(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: ["email"],
                        properties: {
                            file_url: {
                                type: "string",
                            },
                            email: {
                                type: "string",
                            },
                            callback_url: {
                                type: "string"
                            },
                            overwrite: {
                                type: "boolean"
                            },
                            input_type: {
                                type: "string"
                            },
                            input_data: {
                                type: "array"
                            },
                            platform: {
                                type: "string"
                            }
                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        try {
            const requestBody = request?.body;
            const requestHeaders = request?.headers;
            const authorizationToken = requestHeaders.authorization
            const fileOverwrite = requestBody.overwrite || false;
            const decodedToken = await jwt.verify(authorizationToken, crmSecretKey);
            const email = requestBody?.email;
            const emailFromToken = decodedToken.email;
            const platform = requestBody.platform;
            
            if (emailFromToken != email) {
                return response.status(400).send({ msg: 'Invalid token', data: null });
            }

            const processFileUrl = requestBody?.file_url;
            const callbackUrl = requestBody?.callback_url;
            let inputData = requestBody?.input_data;
            let input_type = requestBody?.input_type || 'URL';
            let filename = 'fileName';
            let jsonFilePath = null;
            let hashValue = null;
            let fileHistoryRepositoryWhereObj: any = {
                email
            }

            if (input_type == 'JSON') {
                hashValue = await createHash(inputData);
                const timestamp = new Date().toISOString().replace(/[-:.]/g, '');
                filename = `leads_sorting_${timestamp}.csv`.toLowerCase(); // Add timestamp to the file name
                // fileHistoryRepositoryWhereObj.filename = filename;
                fileHistoryRepositoryWhereObj.input_data_hash = hashValue;

            } else {
                fileHistoryRepositoryWhereObj.process_file_url = processFileUrl;
            }
            const customerDataPromise = this.customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: {
                    email,
                }
            })
            const fileHistoryDataPromise = this.fileHistoryRepository.findOne({
                where: fileHistoryRepositoryWhereObj
            })
            const [customerData, fileHistoryData] = await Promise.all([customerDataPromise, fileHistoryDataPromise]);
            if (!customerData) {
                return response.status(404).send({ msg: 'Customer data not found', data: null });
            }
            if (!fileHistoryData || fileOverwrite) {
                let fileHistoryObj;

                if (fileOverwrite && fileHistoryData) {
                    filename = fileHistoryData.filename;
                    fileHistoryObj = fileHistoryData;
                    fileHistoryObj.status = 1;
                    fileHistoryObj.error_detail = undefined;
                    fileHistoryObj.error = undefined;
                    await this.fileHistoryRepository.update(fileHistoryObj);
                } else {
                    if (input_type == 'JSON' && (inputData && inputData.length)) {
                        jsonFilePath = path.join(__dirname, `../../.sandbox/${filename}`); // Path to and name of object. For example '../myFiles/index.js'.
                        await convertJsonToCsv(inputData, jsonFilePath);
                        try {
                            let newfileStream = fs.createReadStream(jsonFilePath);
                            await UploadS3(jsonFilePath, newfileStream, email);
                            if (fs.existsSync(jsonFilePath)) {
                                fs.unlinkSync(jsonFilePath);
                            }
                            console.log('Successfully wrote to CSV file');
                        } catch (err) {
                            console.error('Error writing to CSV file', err);
                        }

                    }
                    let leadSource = platform || customerData?.source ;
                    fileHistoryObj = new FileHistory({
                        email,
                        input_type,
                        input_data_hash: hashValue || '',
                        filename: filename,
                        file_extension: 'file_extension',
                        upload_date: new Date(),
                        record_count: 0,
                        status: 0,
                        process_file_url: processFileUrl,
                        meta_data: {
                            callback_url: callbackUrl,
                            third_party_sorting: true,
                            status: "PROCESSING",
                            platform
                        },
                        source: leadSource
                    });
                    fileHistoryObj = await this.fileHistoryRepository.create(fileHistoryObj)
                }

                const fileUUID = fileHistoryObj.id || '';
                // token to check status of processed file will be valid for 7 days
                const newToken = jwt.sign({
                    email,
                    fileUUID
                }, crmSecretKey, { expiresIn: '48h' });
                const options = { url: processFileUrl, filename }
                CRMIntegrationService.processFile(options, customerData, fileHistoryObj, this.customerRepository
                    , this.fileHistoryRepository, this.customerIndustryRepository, this.transactionHistoryRepository,
                    this.adminEventsRepository, this.integrationsRepository,this.generateLeadsRepository);

                return response.status(200).send({ msg: 'File processing started', data: { token: newToken, file_upload_identifier: fileUUID } });

            } else {
                const fileUUID = fileHistoryData.id || '';
                const newToken = jwt.sign({
                    email,
                    fileUUID
                }, crmSecretKey, { expiresIn: '48h' });
                if (fileHistoryData?.status !== 7) {
                    let { statusCode, formatResponse, status } = CRMIntegrationService.formatResponse(fileHistoryData.error_detail || '', fileHistoryData.error || '')
                    return response.status(statusCode).send({ msg: formatResponse, data: { error_detail: statusCode == 200 ? formatResponse : null, token: newToken, file_upload_identifier: fileHistoryData.id, status } });
                }
                else {
                    const { msg, data } = await CRMIntegrationService.getProcessedData(fileHistoryData, email)
                    return response.status(data.statusCode).send({ msg, data });
                }
            }

        } catch (error) {
            console.log(error);
            return response.status(500).send({ msg: error.message });
        }
    }


    // @authenticate('jwt')
    @post(`${baseUrl}/file-status`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'File Status',
            },
        },
    })
    async fileUploadStatus(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: ["file_upload_identifier", "email"],
                        properties: {
                            file_upload_identifier: {
                                type: "string",
                            },
                            email: {
                                type: "string",
                            },

                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        try {
            const requestBody = request?.body;
            const fileUUID = requestBody.file_upload_identifier
            const email = requestBody?.email;
            const requestHeaders = request?.headers;
            const authorizationToken = requestHeaders.authorization
            const decodedToken = await jwt.verify(authorizationToken, crmSecretKey);
            const fileUUIDFromToken = decodedToken.fileUUID;
            const emailFromToken = decodedToken.email;

            if (fileUUIDFromToken != fileUUID || emailFromToken != email) {
                return response.status(400).send({ msg: 'Invalid token', data: null });
            }

            const customerModelData = await this.customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: {
                    email,
                }
            })
            if (customerModelData) {

                let fileHistoryData = await this.fileHistoryRepository.findById(fileUUID);

                if (fileHistoryData?.status !== 7) {
                    let { statusCode, formatResponse, status } = CRMIntegrationService.formatResponse(fileHistoryData.error_detail || '', fileHistoryData.error || '')
                    return response.status(statusCode).send({ msg: formatResponse, data: { error_detail: formatResponse, status } });
                } else {
                    const { msg, data } = await CRMIntegrationService.getProcessedData(fileHistoryData, email)
                    return response.status(data.statusCode).send({ msg, data });
                }

            } else {
                return response.status(404).send({ msg: 'Customer data not found' });
            }

        } catch (error) {
            console.log(error);
            return response.status(500).send({ msg: error.message });
        }
    }


    @authenticate('jwt')
    @post(`${baseUrl}/generate-api-key`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'generate-api-key',
            },
        },
    })
    async generateApiKey(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: ['email'],
                        properties: {
                            email: {
                                type: "string",
                            },

                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        try {
            const requestBody = request?.body;
            const requestHeaders = request?.headers;
            const email = requestBody.email as string;
            if (!email)
                return response.status(400).send({ msg: 'Email is missing in headers', data: {} });
            const customerData = await this.customerRepository.findOne({
                fields: { login_history: false, file_history: false },
                where: {
                    email,
                }
            })
            if (customerData) {
                customerData.api_secret_key = generateApiKey();
                await this.customerRepository.update(customerData);
                return response.status(200).send({ msg: 'Api key generated successfully', data: { api_key: customerData.api_secret_key } });

            } else {
                return response.status(404).send({ msg: 'Customer data not found' });
            }

        } catch (error) {
            console.log('error in generate api key', error);
            return response.status(500).send({ msg: error.message });
        }
    }


    @authenticate('jwt')
    @post(`${baseUrl}/add-user`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Add user',
            },
        },
    })
    async addUser(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: ['email', 'password', 'name'],
                        properties: {
                            email: {
                                type: "string",
                            },
                            password: {
                                type: "string",
                            },
                            name: {
                                type: "string",
                            },
                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        try {
            const requestBody = request?.body;
            const requestHeaders = request?.headers;
            const parentEmail = requestHeaders.email as string;
            const email = requestBody.email as string;
            const password = requestBody.password as string;
            const name = requestBody.name as string;
            if (!(email && password && name && parentEmail))
                return response.status(400).send({ msg: 'Required parameters are missing in headers', data: {} });

            const customerCollection = (this.customerRepository.dataSource.connector as any).collection("Customer")
            // const subAccountPromise = customerCollection.aggregate([
            //     {
            //         '$match': {
            //             'sub_accounts': {
            //                 '$elemMatch': {
            //                     'email': email
            //                 }
            //             }
            //         }
            //     }
            // ]).toArray()
            const customerDataPromise = this.customerRepository.findOne({
                where: {
                    email
                }
            })
            // const [subAccountData, customerData] = await Promise.all([subAccountPromise, customerDataPromise])
            const [customerData] = await Promise.all([customerDataPromise])

            if (customerData) {
                return response.status(400).send({ msg: 'Email id already registered', data: {} });
            } else {
                const parentData = await this.customerRepository.findOne({
                    where: {
                        email: parentEmail
                    }
                })
                if (parentData) {

                    try {

                        // auth0 user creation
                        const oauthTokenData = await getOauthToken();
                        const oauthToken = oauthTokenData?.data?.access_token;

                        const axios = require('axios');
                        let data = JSON.stringify({
                            "email": email,
                            "user_metadata": {
                                remark: "user added through app"
                            },
                            "app_metadata": {},
                            "name": name,
                            "connection": "Username-Password-Authentication",
                            "password": password
                        });

                        let config = {
                            method: 'post',
                            maxBodyLength: Infinity,
                            url: `${process.env.AUTH0_DOMAIN}/api/v2/users`,
                            headers: {
                                'Content-Type': 'application/json',
                                'Accept': 'application/json',
                                'Authorization': `Bearer ${oauthToken}`
                            },
                            data
                        };
                        let addUserData;
                        try {
                            addUserData = await axios.request(config);
                        } catch (error) {
                            console.error("error while creating user on oauth's end", error.message);
                            return response.status(409).send({ msg: 'The user already exists', data: {} });
                        }

                        // const childAcc = { email, name };
                        let customer = new Customer;
                        customer.email = email;
                        customer.name = name;
                        // customer.stripe_customer_id = await CustomerService.create_stripe_customer(email);
                        customer.stripe_customer_id = parentData.stripe_customer_id;
                        customer.api_secret_key = generateApiKey();
                        customer.parent_email = parentEmail;
                        customer.pricing_plan = parentData.pricing_plan;
                        customer.login_history = [{ last_login: new Date().toString() }];

                        this.customerRepository.create(customer);
                        const customerIndustryData = await this.customerIndustryRepository.findOne({
                            where: {
                                email: parentEmail
                            }
                        })
                        const customerIndustryType = {
                            email: customer.email,
                            industry_type: customerIndustryData?.industry_type || industryTypes.REAL_ESTATE_INVESTORS,
                          }
                          
                        const res = await this.customerIndustryRepository.create(customerIndustryType)
                        // if (parentData.sub_accounts) {
                        //     parentData.sub_accounts.push(childAcc);
                        // } else {
                        //     parentData.sub_accounts = [childAcc];
                        // }

                        // await this.customerRepository.update(parentData);
                        return response.status(200).send({ msg: 'User created successfully', data: { name, email } });

                    } catch (error) {
                        // Handle errors
                        const errorMessage = error?.response?.data?.description || error.message;
                        console.error('Error during add user signup with auth0:', errorMessage);
                        return response.status(400).send({ msg: errorMessage, data: {} });
                    }
                } else
                    return response.status(400).send({ msg: 'Invalid email id', data: {} });
            }

        } catch (error) {
            console.log('error in add user', error);
            return response.status(500).send({ msg: error.message, data: {} });
        }
    }


    @authenticate('jwt')
    @get(`${baseUrl}/user-list`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Get User List',
            },
        },
    })
    async userList(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: [],
                        properties: {
                            email: {
                                type: "string",
                            },
                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<object> {
        try {
            const email = request.headers.email as string;
            if (!email)
                return response.status(400).send({ msg: 'Email is missing in headers', data: {} });

            const userLists = await this.customerRepository.find({
                fields: ['email', 'name'],
                where: { parent_email: email },
            });

            return response.send({ msg: 'User List fetched successfully', data: { userLists: userLists || [] } });
        } catch (error) {
            console.log(error);
            return response.status(500).send({ msg: error.message });
        }
    }

    // @authenticate('jwt')
    @get(`${baseUrl}/user-details`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'Get User Details',
            },
        },
    })
    async getUserDetails(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: [],
                        properties: {
                            email: {
                                type: "string",
                            },
                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<object> {
        try {
            const authenticationKey = request.headers.authorization;
            const crmKey = request.headers.apikey as string;
            const email = request.headers.email as string;
            // console.log("Headers from user details api", request.headers)
            if(!authenticationKey){
                return response.status(400).send({
                    data: {},
                    "msg": "You do not have permission to access this resource."
                })
            }

            if (!email)
                return response.status(400).send({ msg: 'Email is missing in headers', data: {} });

            const userDetail = await this.customerRepository.findOne({ where: { email }});
            if (!userDetail)
                return response.status(400).send({ msg: 'User not found', data: {} });

            if (userDetail.api_secret_key !== crmKey) {
                return response.status(404).send({ msg: 'Api key is invalid', data: {} });
            }

            const referralData = await this.referralRepository.findOne({
                where: {
                    integration_key: authenticationKey,
                }
              });

            if(referralData){
              
                let remainingRowsForMonth = 0;
                let totalRowForMonth = 0;
                let results: any;
                let calculateRowsLeftPromise = calculateRowsLeftForUser(userDetail, this.fileHistoryRepository);
                [results] = await Promise.all([calculateRowsLeftPromise]);
                remainingRowsForMonth = results.remainingRowsForMonth;
                totalRowForMonth = results.totalAllowedRowCount;

                let last4Digit = null
                last4Digit = await stripPaymentInfo(userDetail);

                return response.send({ msg: 'Customer data fetched successfully', data: { 
                    // id: userDetail?.id,
                    name: userDetail?.name,
                    email:userDetail?.email,
                    plan: userDetail?.pricing_plan?.plan || null,
                    planStatus: userDetail?.pricing_plan?.stripe_subscription_status || null,
                    paymentType: last4Digit?.cardInfo?.last4 ? "credit_card" : null,
                    remainingCreditsForCurrentMonth: remainingRowsForMonth || 0,
                    totalAllowedRowCount: totalRowForMonth || 0,
                    // signupDate: userDetail?.login_history?.length && (userDetail?.login_history[0] as any)?.last_login,
                    ccLast4Digit: last4Digit?.cardInfo?.last4 || null,
                    brand: last4Digit?.cardInfo?.brand || null,
                } });
            }
            else {
                return response.status(400).send({
                    data: {},
                    "msg": "You do not have permission to access this resource."
                })
            }
        } catch (error) {
            console.log(error);
            return response.status(500).send({ msg: error.message });
        }
    }

    // #securityTodo
    // @authenticate('jwt')
    @post(`${baseUrl}/user-authenticate`, {
        responses: {
            200: {
                content: {
                    'application/json': {
                        schema: {
                            type: 'object',
                        },
                    },
                },
                description: 'user-authenticate',
            },
        },
    })
    async authenticateUser(
        @requestBody({
            content: {
                "application/json": {
                    schema: {
                        type: "object",
                        required: ['email', 'password'],
                        properties: {
                            email: {
                                type: "string",
                            },
                            password: {
                                type: "string",
                            },
                        },
                    },
                },
            },
        })
        @inject(RestBindings.Http.REQUEST)
        request: Request,
        @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        try {
            const requestBody = request?.body;
            const email = requestBody.email as string;
            const password = requestBody.password as string;
            if (!(email && password))
                return response.status(400).send({ msg: 'Required parameters are missing in body', data: {} });

            const customerData = await this.customerRepository.findOne({
                where: {
                    email
                }
            })

            if (customerData) {
                try {
                    const oauthTokenData = await checkOauthUser(email, password);
                    if (oauthTokenData.statusCode == 200) {
                        // if source is not set then update in customer collection
                        if (!customerData.source) {
                            customerData.source = apiUsers.LPG;
                            await this.customerRepository.update(customerData);
                        }
                        return response.status(200).send({ msg: oauthTokenData.msg, data: { api_secret_key: customerData.api_secret_key } });
                    }
                    else
                        return response.status(oauthTokenData.statusCode).send({ msg: oauthTokenData.msg, data: { error_detail: oauthTokenData?.data.message } });

                } catch (error) {
                    // Handle errors
                    const errorMessage = error?.response?.data?.description || error.message;
                    console.error('Error during add user signup with auth0:', errorMessage);
                    return response.status(400).send({ msg: errorMessage, data: {} });
                }
            } else {
                return response.status(400).send({ msg: 'Invalid Email id', data: {} });
            }

        } catch (error) {
            console.log('error in add user', error);
            return response.status(500).send({ msg: error.message, data: {} });
        }
    }
}
