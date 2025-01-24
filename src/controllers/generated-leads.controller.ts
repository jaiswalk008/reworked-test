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

import { AdminEventsRepository, CustomerIndustryRepository, CustomerModelsRepository, CustomerRepository, FileHistoryRepository, GenerateLeadsRepository, IntegrationsRepository, TransactionHistoryRepository } from "../repositories";
import { TokenServiceBindings } from '@loopback/authentication-jwt';
import { TokenService, authenticate } from '@loopback/authentication';
import { GenerateLeadArgs } from '../interface/generate-leads.interface';
import { repository } from "@loopback/repository";
import { GenerateLeadsModel } from "../models";
import { GenerateLeadsService } from "../services/generated-leads.service";
import { fetchCountyStateFromCSV } from '../data/fetch_data'
import { updateStripeDefaultPayment } from '../helper/stripe-payment';
import { getPriceFromRange, perUnitCostLeadGeneration } from '../helper';
const baseUrl = '/generated-leads'

const testingEmailsForLeadGen = ["umdterp8488@gmail.com"]
export class GeneratedLeadsController {
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
    @repository(AdminEventsRepository)
    public adminEventsRepository: AdminEventsRepository,
    @repository(TransactionHistoryRepository)
    public transactionHistoryRepository: TransactionHistoryRepository,
    @repository(FileHistoryRepository)
    public fileHistoryRepository: FileHistoryRepository,
    @repository(CustomerIndustryRepository)
    public customerIndustryRepository : CustomerIndustryRepository,
    @repository(IntegrationsRepository)
    public integrationsRepository : IntegrationsRepository,
  ) { }


  @authenticate('jwt')
  @post(`${baseUrl}`, {
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
  async generateList(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["count", "places", "total_cost", "nation_wide"],
            properties: {
              count: {
                type: "number",
              },
              total_cost: {
                type: "number",
              },
              payment_method_id: {
                type: "string",
              },
              places: {
                "type": "array",
                "items": {
                  "type": "string"
                }
              },
              nation_wide: {
                type: "boolean"
              },
              model_name: {
                type: "string"
              },
              default_model: {
                type: "boolean"
              },
              zip_codes: {
                "type": "array",
                "items": {
                  "type": "string"
                }
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
      // const requestHeaders = request?.headers;
      let leadsCount = parseInt(requestBody?.count);
      const stripePaymentMethodId = requestBody?.stripe_payment_method_id;
      const placeList = requestBody?.places;
      const nationWide = (requestBody?.places?.length || requestBody?.zip_codes?.length ) ? false : true;
      const defaultModel = requestBody?.default_model || false;
      const email = request.headers.email as string;
      const modelName = requestBody.model_name as string;
      const zipCodes = requestBody?.zip_codes as string [];
      let totalCost = parseInt(requestBody.total_cost) as number;
      // test email account for lead generation. Hardcoding lead count to 10 instead of 1000
      if (testingEmailsForLeadGen.includes(email)) {
        leadsCount = 10;
      }

      let generateLeadsModelData = new GenerateLeadsModel({
        email: request.headers.email as string,
        model_name: modelName,
        lead_count: leadsCount,
        status: 1,
        place_list: placeList,
        amount_spent: totalCost,
        default_model: defaultModel,
        zip_codes:zipCodes
      });
      const customerData = await this.customerRepository.findOne({ where: { email } });
      if (!customerData) {
        return response.status(404).send({ msg: 'Invalid email id' });
      }
      let leadGenRowCredits = customerData.lead_gen_row_credits || 0;
      if (totalCost>0 &&  stripePaymentMethodId) {
        // attach default payment id
        await updateStripeDefaultPayment(customerData, stripePaymentMethodId, this.customerRepository);
      }

      if (leadsCount <= leadGenRowCredits) {
        leadGenRowCredits -= leadsCount;
        customerData.lead_gen_row_credits = leadGenRowCredits
        // await this.customerRepository.update(customerData)
        totalCost = 0;
      } else {
        const extraCredits = leadsCount - leadGenRowCredits;
        const perUnitCostAndRangeLeadGeneration = perUnitCostLeadGeneration['payasyougo'];
        const finalAmount = await getPriceFromRange(extraCredits, perUnitCostAndRangeLeadGeneration);
        totalCost = finalAmount;
        customerData.lead_gen_row_credits = 0;
      }
      const updateCustomerPromise = this.customerRepository.update(customerData);

      const createGenerateLeadsPromise = this.generateLeadsRepository.create(generateLeadsModelData);
      let findCustomerModelPromise = null;
      if (modelName) {
        findCustomerModelPromise = this.customerModelsRepository.findOne({
          where: {
            email,
            name: modelName
          }
        });
      } else {
        // If modelName is null, resolve with undefined to keep consistent array structure
        findCustomerModelPromise = Promise.resolve(undefined);
      }

      const [updatedCustomerData, generateLeadsModelDataCreated, customerModelData] = await Promise.all([updateCustomerPromise, createGenerateLeadsPromise, findCustomerModelPromise])
      if (customerModelData || defaultModel) {
        const generateLeadArgs: GenerateLeadArgs = { stripePaymentMethodId, leadsCount, placeList: placeList,nationWide, email, modelName, totalCost, defaultModel ,zipCodes };
        const { msg, data, status } = await GenerateLeadsService.generateLeads(generateLeadArgs, customerModelData, this.generateLeadsRepository, this.customerRepository,
          this.adminEventsRepository, this.transactionHistoryRepository, generateLeadsModelDataCreated, customerData,this.fileHistoryRepository,this.customerIndustryRepository , this.integrationsRepository
        );

        return response.status(status).send({ msg, data });
      } else {
        return response.status(404).send({ msg: 'Customer model data not found' });
      }

    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }


  @authenticate('jwt')
  @post(`${baseUrl}/leads-availability`, {
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
  async leadsAvailability(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["count", "places"],
            properties: {
              count: {
                type: "number",
              },
              "places": {
                "type": "array",
                "items": {
                  "type": "string"
                }
              },
              model_name: {
                type: "string",
              },
              default_model: {
                type: "boolean"
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
      // await fetchCountyStateFromCSV();
      let statusCode = 200;
      const requestBody = request?.body;
      // const requestHeaders = request?.headers;
      const defaultModel = requestBody?.default_model || false;
      const leadsCount = requestBody?.count;
      const placeList = requestBody?.places;
      const email = request.headers.email as string;
      const modelName = requestBody.model_name as string;
      let customerModelDataToSend = null;
      // generateLeadsModelData = await this.generateLeadsRepository.create(generateLeadsModelData)

      if (defaultModel) {
        customerModelDataToSend = {};
      }
      else {
        customerModelDataToSend = await this.customerModelsRepository.findOne({
          where: {
            email,
            name: modelName
          }
        })
      }
      if (customerModelDataToSend) {
        const generateLeadArgs: any = { leadsCount, placeList: placeList, email, modelName, defaultModel };
        const customerIndustryDetails = await this.customerIndustryRepository.findOne({where:{email}})
        const industryType = customerIndustryDetails?.industry_type || ''

        const res: any = await GenerateLeadsService.leadsAvailability(generateLeadArgs, customerModelDataToSend, industryType);
        const leadsAvailableMsg = "Leads are not available"
        return response.send({
          msg: res.success ? 'Leads are available' : leadsAvailableMsg, data: {
            leads_available: res.success,
            lead_available_count: res?.data?.totalFetchedLeads || 0
            // no_of_leads_available: 1
          }
        });

      } else {
        return response.status(404).send({ msg: 'Customer model data not found' });
      }

    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }

  @authenticate('jwt')
  @post(`${baseUrl}/get-leads-history`, {
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
  async getGeneratedLeadsList(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: [],
            properties: {
              model_name: {
                type: "string",
              },
              email: {
                type: "string",
              },
              default_model: {
                type: "boolean"
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

      const modelName: string | undefined = request.body.model_name as string;
      const email = request.headers.email as string;
      const defaultModel = request.body?.default_model || false;
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
      if (!leadsEmail) {
        return response.status(403).send({
          msg: 'Email id required',
          data: null
        })
      }
      const geneatedLeadsList = await GenerateLeadsService.getGeneratedListHistory(leadsEmail, this.customerRepository, this.generateLeadsRepository, modelName, idAdmin, defaultModel);
      return response.send({ msg: 'Generated List fetched successfully', data: { geneatedLeadsList } });
    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }

  }

  @authenticate('jwt')
  @post(`${baseUrl}/get-all-leads-history`, {
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
  async getAllGeneratedLeadsList(
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
      if (!leadsEmail) {
        return response.status(403).send({
          msg: 'Email id required',
          data: null
        })
      }
      const geneatedLeadsList = await GenerateLeadsService.getAllGeneratedListHistory(request, this.generateLeadsRepository);
      return response.send({ msg: 'Generated List fetched successfully', data: { geneatedLeadsList } });
    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }

  }


  @authenticate('jwt')
  @post(`${baseUrl}/retry-leads-file-generation`, {
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
  async retryGenerateLeadsFile(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["generateLeadsId"],
            properties: {
              generateLeadsId: {
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
      const generateLeadsId = requestBody?.generateLeadsId;
      let adminEmail = request.headers.email as string;

      if (adminEmail) {
        let adminData = await this.customerRepository.findOne({
          fields: ['email'],
          where: { role: 'admin', email: adminEmail },
        });
        if (!adminData) {
          return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
        }
      }

      const email = requestBody.email as string;
      const generateLeadsDataPromise = this.generateLeadsRepository.findById(generateLeadsId);
      const customerDataPromise = this.customerRepository.findOne({
        where: {
          email
        }
      });
      const [generateLeadsData, customerData] = await Promise.all([generateLeadsDataPromise, customerDataPromise]);
      if (!(generateLeadsData && customerData) && generateLeadsData.status != 2) {
        return response.status(404).send({ msg: 'Invalid request' });
      }
      const retryFlag = true;
      if (generateLeadsData.leads_api_options) {
        GenerateLeadsService.processMelissaCSV(generateLeadsData, this.generateLeadsRepository, customerData,this.fileHistoryRepository,this.customerRepository,this.customerIndustryRepository,this.transactionHistoryRepository,this.adminEventsRepository,this.integrationsRepository, retryFlag);
        return response.status(200).send({ msg: "Retry request submitted successfully", data: generateLeadsData });
      }
      else {
        return response.status(400).send({ msg: "Invalid request", data: null });
      }


    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }

}

