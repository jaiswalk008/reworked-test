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
  CustomerModelsRepository, CustomerRepository, CustomerIndustryRepository, FileHistoryRepository,
  TransactionHistoryRepository, AdminEventsRepository
} from "../repositories";

import { TokenServiceBindings } from '@loopback/authentication-jwt';
import { TokenService, authenticate } from '@loopback/authentication';
import { industryTypes } from "../constant/industry_type";
import { repository } from "@loopback/repository";
import path from "path";
import fs from "fs";
import { FileUploadHandler } from "../types";
import { FILE_UPLOAD_SERVICE } from "../keys";
import { runPythonScript, UploadS3, CustomerModelsService, FileUploadProvider } from "../services";
import { ScriptOutput } from "../constant/script_output";
import { CustomerModels, FileHistory } from "../models";
import { stripePayment, updateStripeDefaultPayment } from '../helper/stripe-payment';
import { extractPythonResponse } from "../helper/utils";
import { sendEmailToAdmin, sendMailChimpEmail } from "../helper"
// import { downloadFileFromS3, DownloadS3, ListS3, runPythonScript, UploadS3 } from '../services';
import stripeClient from "../services/stripeClient";

const baseUrl = '/customer_models'
interface File {
  originalname: string;
  encoding: string;
  mimetype: string;
  buffer: Buffer;
  path: string;
}

export class CustomerModelsController {
  /**
   * Constructor
   * @param customerRepository
   * @param fileHistoryRepository
   * @param handler - Inject an express request handler to deal with the request
   */
  constructor(
    @inject(TokenServiceBindings.TOKEN_SERVICE)
    public jwtService: TokenService,
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
    protected adminEventsRepository: AdminEventsRepository,
    @inject(FILE_UPLOAD_SERVICE) private handler: FileUploadHandler
  ) { }

  @authenticate('jwt')
  @post(`/customer_models-paginated`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Get Customer Models',
      },
    },
  })
  async getCustomerModelsPaginated(
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
      const modelType = request.body.model_type as string;

      let leadsEmail = email;
      let idAdmin = false;
      let adminEmail = request.body.email as string;
      if (adminEmail) {
        leadsEmail = adminEmail;
        idAdmin = true;
        if (!adminEmail) adminEmail = ''
        const adminData = await this.customerRepository.findOne({
          fields: ['email'],
          where: { role: 'admin', email: adminEmail },
        });
        if (!adminData) return response.status(403).send({
          msg: 'Request should be made by admin',
          data: null
        })
      }
      const customerModelData = await CustomerModelsService.getModelsPaginated(request, leadsEmail, this.customerRepository, this.customerModelsRepository, modelType, idAdmin);
      return response.send({ msg: 'Models fetched successfully', data: { models: customerModelData } });
    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }

  @authenticate('jwt')
  @get(`${baseUrl}`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Get Customer Models',
      },
    },
  })
  async getCustomerModels(
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
      const modelType = request.query.type as string;

      let leadsEmail = email;
      let idAdmin = false;
      let adminEmail = request.body.email as string;
      if (adminEmail) {
        leadsEmail = adminEmail;
        idAdmin = true;
        if (!adminEmail) adminEmail = ''
        const adminData = await this.customerRepository.findOne({
          fields: ['email'],
          where: { role: 'admin', email: adminEmail },
        });
        if (!adminData) return response.status(403).send({
          msg: 'Request should be made by admin',
          data: null
        })
      }
      const customerModelData = await CustomerModelsService.getAllModels(request, leadsEmail, this.customerRepository, this.customerModelsRepository, modelType, idAdmin);
      return response.send({ msg: 'Models fetched successfully', data: { models: customerModelData } });
    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }

  @authenticate('jwt')
  @post(`${baseUrl}/get-models`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Get Generated List',
      },
    },
  })
  async getAllModels(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: [],
            properties: {
              model_type: {
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
  ): Promise<object> {
    try {
      const email = request.headers.email as string;
      const modelType = request.body.model_type as string;

      let leadsEmail = email;
      let idAdmin = false;
      let adminEmail = request.body.email as string;
      if (adminEmail) {
        leadsEmail = adminEmail;
        idAdmin = true;
        if (!adminEmail) adminEmail = ''
        const adminData = await this.customerRepository.findOne({
          fields: ['email'],
          where: { role: 'admin', email: adminEmail },
        });
        if (!adminData) return response.status(403).send({
          msg: 'Request should be made by admin',
          data: null
        })
      }
      const customerModelData = await CustomerModelsService.getAllModels(request, leadsEmail, this.customerRepository, this.customerModelsRepository, modelType, idAdmin);
      return response.send({ msg: 'Models fetched successfully', data: { models: customerModelData } });
    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }

  }

  @authenticate('jwt')
  @post(`${baseUrl}`, {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
            },
          },
        },
        description: "Create Customer Model",
      },
    },
  })
  async newFileUpload(
    @requestBody.file()
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        else {
          let paymentRequired = true;
          const email = request.headers.email as string;
          const modelName: string = request.body.modelName as string;
          const modelDescription: string = request.body.description as string;
          const stripePaymentMethodId = request.body.stripe_payment_method_id;
          // const userFeatureColumns = request.body.user_feature_columns || null;
          const featureColumnsString = request?.body?.feature_columns || ''; // Ensure it's a string
          const featureColumns = featureColumnsString ? JSON.parse(featureColumnsString) : [];
          const files = request.files as { [fieldname: string]: File[]; };
          const file = Array.isArray(files) ? files[0] : files;

          const modelFilename = file.filename;
          const fileFormat: string = modelFilename.split(".").pop()!;

          const modelFilePath = `${file.destination}/${modelFilename}`;

          const industryProfileId = request?.body?.industry_profile_id;
          let industrialProfile = [];
          let industrialProfileQuestionnaire: any = {};

          const findbyemailPromise = this.customerRepository.findOne({
            fields: { login_history: false, file_history: false },
            where: { email },
          });

          const findIndustryProfilePromise = this.customerIndustryRepository.findOne({ where: { email} });

          const findCustomerModelsPromise = this.customerModelsRepository.find({
            where: {
              email,
              type: 'lead_generation'
            }
          });

          const [findbyemail, findIndustryProfile, customerModels] = await Promise.all([findbyemailPromise, findIndustryProfilePromise, findCustomerModelsPromise]);

          const industryProfile = findIndustryProfile?.industry_profile || []

          if (industryProfileId)
            industrialProfile = industryProfile.filter((ele: { id: any; }) => ele.id == industryProfileId)
          else
            industrialProfile = industryProfile.filter((ele: { default: any; }) => ele.default == true)

          if (industrialProfile?.length) {
            industrialProfileQuestionnaire = industrialProfile && industrialProfile.length && industrialProfile[0].question_answers || {};
          }
          if (findbyemail) {
            const industryType = request.body.industry_type || industrialProfile[0]?.question_answers?.industryType;
            let customer_model_obj = new CustomerModels({
              email,
              name: modelName,
              description: modelDescription,
              vendor_list_url: modelFilename,
              status: 1,
              file_extension: ".csv",
              type: 'lead_generation',
            //   industry_type: industryType,
              industry_profile: industrialProfileQuestionnaire,
              industry_profile_id: industryProfileId,
              insights: {
                feature_columns: featureColumns,
                nation_wide_count: '',
                top_zip_code: {}
              },
            });
            // first model should process without payment
            if (customerModels?.length > 1) {
              paymentRequired = true;
            }
            let args = ["--file_path", file.path, "--file_format", fileFormat, "--output_path", modelFilePath];
            let scriptPath = path.join(__dirname, '../../python_models/parseUploadFile.py');

            // Script parses uploaded xls and csv files and makes new output by removing completely empty rows and stores it into a csv file of the same name as uploaded name in .sandbox folder
            runPythonScript(scriptPath, args).then(async (python_output: any) => {
              let { output } = await extractPythonResponse({ python_output });

              // upload model file to s3
              await UploadS3(modelFilename, fs.createReadStream(modelFilePath), email);

              if (fileFormat == 'xls' || fileFormat == 'xlsx')
                fs.unlinkSync(file.path);

              customer_model_obj.row_count = (output.mapped_cols as any)?.row_count;
              if (output.success !== 'True') {

                customer_model_obj.error_detail = output.error_details;
                customer_model_obj.error = 'Parse Upload File Error';

                if (fs.existsSync(file.path)) {
                  fs.unlinkSync(file.path);
                }
                customer_model_obj = await this.customerModelsRepository.create(customer_model_obj)
                const optionsforAdminMail = {
                  error: customer_model_obj.error,
                  errorDetails: customer_model_obj.error_detail,
                  content: `Model creation failed with modelname: ${modelName} and file: ${modelFilename} with error: ${customer_model_obj.error_detail}`
                }
                await sendEmailToAdmin(modelFilename, findbyemail, this.customerRepository, optionsforAdminMail)

                return
              }
              else {
                customer_model_obj.status = 2;
                if (customerModels?.length > 1) {

                  let totalAmount = 45;
                  let metaData = { modelId: customer_model_obj.id };
                  let noOfCredits = request.body?.no_of_credits;

                  const args = {
                    totalAmount, metaData, noOfCredits, email, payment_type: "Model Payment",
                    invoiceItemDescription: "New model creation"
                  };

                  // update default payment method id
                  let updateStripeDefaultPaymentResponse = null;
                  if (stripePaymentMethodId) {
                    updateStripeDefaultPaymentResponse = await updateStripeDefaultPayment(findbyemail, stripePaymentMethodId, this.customerRepository)
                  }

                  if (updateStripeDefaultPaymentResponse && updateStripeDefaultPaymentResponse.statusCode == 500) {
                    customer_model_obj.error_detail = updateStripeDefaultPaymentResponse.msg;
                    customer_model_obj.error = updateStripeDefaultPaymentResponse.msg;

                    await this.customerModelsRepository.create(customer_model_obj)
                    return
                  }
                  let responseFromStripePayment = await stripePayment(args, findbyemail, this.customerRepository, this.transactionHistoryRepository
                    , this.adminEventsRepository)
                  let data = responseFromStripePayment?.data;
                  let msg = responseFromStripePayment?.msg;
                  let statusCode = responseFromStripePayment?.statusCode;
                  // error from stripe payment function
                  if (statusCode == 500) {
                    customer_model_obj.error_detail = msg;
                    customer_model_obj.error = msg;

                    await this.customerModelsRepository.create(customer_model_obj)
                    return
                  }

                  // if payment successfully then true
                  if (statusCode == 200)
                    paymentRequired = false;

                } else paymentRequired = false;
                customer_model_obj = await this.customerModelsRepository.create(customer_model_obj)
                if (!paymentRequired)
                  CustomerModelsService.fileValidateService(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, email, modelFilename)
              }

            }).catch((e) => {
              console.error("error", e)
              if (fs.existsSync(file.path)) {
                fs.unlinkSync(file.path);
              }
              reject(e)
            })

            return response.send({
              success: true,
              message: "File has been uploaded successfully",
              data: {
                model_id: customer_model_obj?.id,
                payment_required: paymentRequired
              }
            })
          } else {
            return response.status(500).send({
              success: false,
              message: "Invalid email id",
            })
          }
        }
      });
    });
  }

  @authenticate('jwt')
  @post(`/sort_leads_create_model`, {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
            },
          },
        },
        description: "Create Customer Model",
      },
    },
  })
  async sortLeadsCreateModel(
    @requestBody.file()
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        else {

          let paymentRequired = true;
          const email = request.headers.email as string;
          const files = request.files as { [fieldname: string]: File[]; };
          const existingCustomerfile = Array.isArray(files) ? files[0] : files;
          const existingOrginalFileName = existingCustomerfile.filename;
          let { default: defaultModel, modelName, description: modelDescription, stripe_payment_method_id: stripePaymentMethodId } = request.body;
          defaultModel = defaultModel == 'true' ? true : false;
          const industryType = request?.body?.industry_type;
          const industryProfileId = request?.body?.industry_profile_id;
          const featureColumnsString = request?.body?.feature_columns || ''; // Ensure it's a string
          const featureColumns = featureColumnsString ? JSON.parse(featureColumnsString) : [];
          const moduleType = 'v1.1';
          const fileFormat: string = existingOrginalFileName.split(".").pop()!;

          let customer_model_obj = new CustomerModels({
            email,
            name: modelName,
            description: modelDescription,
            vendor_list_url: existingOrginalFileName,
            type: 'lead_sorting',
            status: 1,
            file_extension: ".csv",
            // industry_type: industryType,
            default: defaultModel,
            insights: {
              feature_columns: featureColumns,
              nation_wide_count: '',
              top_zip_code: {}
            },
            industry_profile_id: industryProfileId
          });
          const modelFilePath = `${existingCustomerfile.destination}/${existingOrginalFileName}`;

          const customerModel = await this.customerModelsRepository.create(customer_model_obj);
          // const featureColumns = request?.body?.feature_columns ? JSON.parse(request?.body?.feature_columns) : [];
          console.log("request.body", request.body)
          const modelDetails = {
            modelName,
            modelDescription,
            defaultModel,
            industryType,
            industryProfileId,
            featureColumns,
            moduleType
          }

          const findbyemailPromise = this.customerRepository.findOne({
            fields: { login_history: false, file_history: false },
            where: { email },
          });

          const findCustomerModelsPromise = this.customerModelsRepository.find({
            where: {
              email,
              type: 'lead_sorting'
            }
          });

          const [findbyemail, customerModels] = await Promise.all([findbyemailPromise, findCustomerModelsPromise]);

          if (findbyemail) {

            let args = ["--file_path", existingCustomerfile.path, "--file_format", fileFormat, "--output_path", modelFilePath];
            let scriptPath = path.join(__dirname, '../../python_models/parseUploadFile.py');
            runPythonScript(scriptPath, args).then(async (ele: any) => {
              const output: ScriptOutput = JSON.parse(ele[0].replace(/'/g, '"'))

              // upload model file to s3
              await UploadS3(existingOrginalFileName, fs.createReadStream(modelFilePath), email);

              if (fileFormat == 'xls' || fileFormat == 'xlsx')
                fs.unlinkSync(existingCustomerfile.path);

              customer_model_obj.row_count = (output.mapped_cols as any)?.row_count;
              if (output.success !== 'True') {

                customer_model_obj.error_detail = output.error_details;
                customer_model_obj.error = output.error;

                if (fs.existsSync(existingCustomerfile.path)) {
                  fs.unlinkSync(existingCustomerfile.path);
                }
                customer_model_obj = await this.customerModelsRepository.create(customer_model_obj)
                return
              } else {
                // CustomerModelsService.leadSortingModelCreationPreprocess(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, email, existingOrginalFileName, modelDetails)
                // first model should process without payment
                if (customerModels?.length > 1) {
                  let totalAmount = 45;
                  let metaData = { modelId: customer_model_obj.id };
                  let noOfCredits = null;

                  const args = {
                    totalAmount, metaData, noOfCredits, email, payment_type: "Model Payment",
                    invoiceItemDescription: "New model creation"
                  };
                  let updateStripeDefaultPaymentResponse = null;
                  if (stripePaymentMethodId) {
                    updateStripeDefaultPaymentResponse = await updateStripeDefaultPayment(findbyemail, stripePaymentMethodId, this.customerRepository)
                  }

                  if (updateStripeDefaultPaymentResponse && updateStripeDefaultPaymentResponse.statusCode == 500) {
                    customer_model_obj.error_detail = updateStripeDefaultPaymentResponse.msg;
                    customer_model_obj.error = updateStripeDefaultPaymentResponse.msg;

                    await this.customerModelsRepository.create(customer_model_obj)
                    return
                  }
                  // const last4Digit = await stripPaymentInfo(findbyemail);
                  let responseFromStripPayment = await stripePayment(args, findbyemail, this.customerRepository, this.transactionHistoryRepository
                    , this.adminEventsRepository)
                  let data = responseFromStripPayment?.data;
                  let msg = responseFromStripPayment?.msg;
                  let statusCode = responseFromStripPayment?.statusCode;
                  if (statusCode == 500) {
                    customer_model_obj.error_detail = responseFromStripPayment.msg;
                    customer_model_obj.error = responseFromStripPayment.msg;

                    await this.customerModelsRepository.create(customer_model_obj)
                    return
                  }
                  // if payment successfully then true
                  if (statusCode == 200)
                    paymentRequired = false;

                } else
                  paymentRequired = false;

                if (!paymentRequired)
                  CustomerModelsService.leadSortingModelCreationFileValidateService(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, email, existingOrginalFileName, modelDetails)
                // CustomerModelsService.leadSortingModelCreationPreprocess(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, email, existingOrginalFileName, modelDetails)
              }
            })
          }

          return response.send({
            success: true,
            message: "File has been uploaded successfully",
            data: {
              model_id: customerModel?.id,
              payment_required: paymentRequired
            }
          })
        }
      });
    });
  }

  @authenticate('jwt')
  @post(`/sort_leads`, {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
            },
          },
        },
        description: "Leads sorting",
      },
    },
  })
  async sortLeads(
    @requestBody.file()
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        else {

          const email = request.headers.email as string;
          const files = request.files as { [fieldname: string]: File[]; };
          const leadsfile = Array.isArray(files) ? files[0] : files;
          const leadsOrginalFileName = leadsfile.filename;
          const modelDetails = {
            modelName: request.body.modelname as string,
          }
          CustomerModelsService.leadSortingFileValidateService(this.customerRepository, this.customerModelsRepository, this.fileHistoryRepository, email, leadsOrginalFileName, modelDetails)
          return response.send({
            success: true,
            message: "File has been uploaded successfully",
          })
        }
      });
    });
  }




  @authenticate('jwt')
  @post('/criteria_generation', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
              // required: ['email', 'filename'],
              properties: {
                // email: {
                //     type: 'string',
                // },
                // filename: {
                //     type: 'string'
                // },
              }
            },
          },
        },
        description: 'Retry criteria generation ',
      },
    },
  })
  async criteriaGeneration(
    @requestBody() fileHandler: object,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {

    let customerEmail = request.headers.email as string;
    let fileName = request.headers.filename as string;
    let isAdmin = false;
    if (request?.body?.email) {
      let adminData = await this.customerRepository.findOne({
        fields: ['email'],
        where: { role: 'admin', email: customerEmail },
      });
      if (adminData) {
        customerEmail = request.body.email as string;
        fileName = request.body.filename as string;
        isAdmin = true;
      } else {
        // return responseToSend;
        return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
      }
    }
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        let responseFromFunction = await CustomerModelsService.criteriaGenerationService(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, customerEmail, fileName);
        resolve(response.status(responseFromFunction.statusCode).send(responseFromFunction));
      });
    });
  }


  @authenticate('jwt')
  @post(`lead_gen_file_validation`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
              }
            },
          },
        },
        description: 'Retry column mapping',
      },
    },
  })
  async columnMapping(
    @requestBody() fileHandler: object,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {

    let customerEmail = request.headers.email as string;
    let fileName = request.headers.filename as string;
    let isAdmin = false;
    if (request?.body?.email) {
      let adminData = await this.customerRepository.findOne({
        fields: ['email'],
        where: { role: 'admin', email: customerEmail },
      });
      if (adminData) {
        customerEmail = request.body.email as string;
        fileName = request.body.filename as string;
        isAdmin = true;
      } else {
        return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
      }
    }
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        let responseFromFunction = await CustomerModelsService.fileValidateService(this.customerRepository, this.customerModelsRepository, this.customerIndustryRepository, customerEmail, fileName);
        resolve(response.status(responseFromFunction.statusCode).send(responseFromFunction));
      });
    });
  }


  @authenticate('jwt')
  @post(`lead_gen_file_preprocess`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
              }
            },
          },
        },
        description: 'Retry file preprocess',
      },
    },
  })
  async filePreProcess(
    @requestBody() fileHandler: object,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {

    let customerEmail = request.headers.email as string;
    let fileName = request.headers.filename as string;
    let isAdmin = false;
    if (request?.body?.email) {
      let adminData = await this.customerRepository.findOne({
        fields: ['email'],
        where: { role: 'admin', email: customerEmail },
      });
      if (adminData) {
        customerEmail = request.body.email as string;
        fileName = request.body.filename as string;
        isAdmin = true;
      } else {
        return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
      }
    }
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        let responseFromFunction = await CustomerModelsService.filePreProcessService(this.customerRepository, this.customerModelsRepository,
          this.customerIndustryRepository, customerEmail, fileName);
        resolve(response.status(responseFromFunction.statusCode).send(responseFromFunction));
      });
    });
  }


  @authenticate('jwt')
  @post(`lead_gen_model_creation`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
              }
            },
          },
        },
        description: 'Retry model creation',
      },
    },
  })
  async modelCreation(
    @requestBody() fileHandler: object,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {

    let customerEmail = request.headers.email as string;
    let fileName = request.headers.filename as string;
    let isAdmin = false;
    if (request?.body?.email) {
      let adminData = await this.customerRepository.findOne({
        fields: ['email'],
        where: { role: 'admin', email: customerEmail },
      });
      if (adminData) {
        customerEmail = request.body.email as string;
        fileName = request.body.filename as string;
        isAdmin = true;
      } else {
        return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
      }
    }
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        let responseFromFunction = await CustomerModelsService.modelCreationService(this.customerRepository, this.customerModelsRepository, 
          this.customerIndustryRepository, customerEmail, fileName);
        resolve(response.status(responseFromFunction.statusCode).send(responseFromFunction));
      });
    });
  }
}


