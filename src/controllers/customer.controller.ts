import { TokenService, authenticate } from "@loopback/authentication";
import { TokenServiceBindings, UserRepository } from "@loopback/authentication-jwt";
import { inject } from "@loopback/core";
import { Count, CountSchema, Filter, FilterExcludingWhere, repository, Where } from "@loopback/repository";
import {
  post,
  param,
  get,
  getModelSchemaRef,
  patch,
  put,
  del,
  Request,
  requestBody,
  response,
  Response,
  RestBindings,
} from "@loopback/rest";
import { Customer, CustomerIndustry, CustomerRelations, FileHistory, GenerateLeadsModel, TransactionHistory } from "../models";
import { AdminEventsRepository, CustomerIndustryRepository, CustomerModelsRepository, CustomerRepository, FileHistoryRepository, 
  PromoRepository, TransactionHistoryRepository, GenerateLeadsRepository, IntegrationsRepository } from "../repositories";
import { SecurityBindings, UserProfile } from "@loopback/security";
import ObjectID from "bson-objectid";
import { CustomerService } from "../services/customer.service";
import { IndustryProfile } from "../types/industry_profile";
import { industryTypes, industrTypesMetaData } from "../constant/industry_type";
import { stripPaymentInfo, stripePayment } from "../helper/stripe-payment";
import { creditsUsedForDates, generateApiKey, getFirstAndLastDateOfMonth, getPriceFromRange, sendEmailToAdmin, sendMailChimpEmail } from "../helper";
import axios from 'axios';
const jwt = require('jsonwebtoken');
import { extractDetailsFromAuthToken } from "../helper/utils";
import { platformIntegrations } from "../constant/platform_integrations";
export class CustomerController {
  constructor(
    @repository(CustomerRepository)
    public customerRepository: CustomerRepository,
    @inject(TokenServiceBindings.TOKEN_SERVICE)
    public jwtService: TokenService,
    @inject(SecurityBindings.USER, { optional: true })
    public user: UserProfile,
    @repository(UserRepository) protected userRepository: UserRepository,
    @repository(FileHistoryRepository)
    protected fileHistoryRepository: FileHistoryRepository,
    @repository(AdminEventsRepository) protected adminEventsRepository: AdminEventsRepository,
    @repository(CustomerIndustryRepository)
    protected customerIndustryRepository: CustomerIndustryRepository,
    @repository(PromoRepository)
    protected promoRepository: PromoRepository,
    @repository(TransactionHistoryRepository)
    public transactionHistoryRepository: TransactionHistoryRepository,
    @repository(CustomerModelsRepository)
    protected customerModelsRepository: CustomerModelsRepository,
    @repository(GenerateLeadsRepository)
    protected generateLeadsRepository: GenerateLeadsRepository,
    @repository(IntegrationsRepository)
    protected integrationsRepository: IntegrationsRepository,
    
  ) { }



  @post("/logins")
  @response(200, {
    description: "Login model instance",
    content: { "application/json": { schema: getModelSchemaRef(Customer) } },
  })
  async create(
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(Customer, {
            title: "NewCustomer",
            exclude: ["id","add_ons","survey_answer"],
          }),
        },
      },
    })
    customer: Omit<Customer, "id">
  ): Promise<{ token: string; cust: object }> {
    let subAccount = 0;
    const findbyemailPromise = this.customerRepository.find({ where: { email: customer.email } });
    // const customerCollection = (this.customerRepository.dataSource.connector as any).collection("Customer")
    // const subAccountPromise = customerCollection.aggregate([
    //   {
    //     '$match': {
    //       'sub_accounts': {
    //         '$elemMatch': {
    //           'email': customer.email
    //         }
    //       }
    //     }
    //   }
    // ]).toArray()
    // let [findbyemail, subAccountData] = await Promise.all([findbyemailPromise, subAccountPromise])
    let [findbyemail] = await Promise.all([findbyemailPromise])
    const userProfile = customer as unknown as UserProfile;
    const token = await this.jwtService.generateToken(userProfile);
    let cust: any;
    let customerId = '';
    let add_ons_needOrderId=findbyemail[0]?.add_ons?.external_order_id || false;
    // if (findbyemail.length > 0 || subAccountData.length > 0) {
    if (findbyemail.length > 0) {

      if (findbyemail[0].parent_email) {
        subAccount = 1;
        const parentAccount =await this.customerRepository.findOne({where:{email:findbyemail[0].parent_email}})
        add_ons_needOrderId =  parentAccount?.add_ons?.external_order_id || false ;
        // findbyemail = subAccountData;
      }
      customerId = findbyemail[0].id || '';
      if (!findbyemail[0].stripe_customer_id) {
        findbyemail[0].stripe_customer_id = await CustomerService.create_stripe_customer(findbyemail[0].email);
        console.log("Created Stripe CustomerID: ", findbyemail[0].stripe_customer_id);
      }
      if (findbyemail[0].login_history) {
        findbyemail[0].login_history.push({ last_login: new Date().toString() });
      } else {
        findbyemail[0].login_history = [{ last_login: new Date().toString() }];
      }
      
      await this.customerRepository.updateById(customerId, findbyemail[0]);
      // await this.customerRepository.update(findbyemail[0], { where: { id: findbyemail[0].id } });
      
      cust = findbyemail[0];
      delete cust["file_history"];
      delete cust["login_history"];
      delete cust["subscription_log"];


    } else {
      // Creating new Customer
      customer.stripe_customer_id = await CustomerService.create_stripe_customer(customer.email);
      customer.api_secret_key = generateApiKey();
      customer.lead_gen_row_credits=0;
      customer.row_credits=0;
      this.customerRepository.create(customer);
      cust = customer;
      // 
      const customerIndustryType = {
        email: customer.email,
        industry_type: industryTypes.REAL_ESTATE_INVESTORS,
      }
      
      const res = await this.customerIndustryRepository.create(customerIndustryType)
      sendMailChimpEmail("welcome-email-ver-1", customer.email, '', customer.name);

    }
    let last4Digit = null;
    const moduleConfigObject = [
      {
        title: "Admin",
        icon: "Adminicon",
        idx: 0,
        level: 1,
        path: "admin",
        display: 0,
      },
      {
        title: "Lead Scoring",
        icon: "AiOutlineCheckCircle",
        idx: 1,
        level: 1,
        path: "leadScoring",
        display: 1,
      },
      {
        title: "Lead Generation",
        icon: "TbBulb",
        idx: 2,
        level: 1,
        path: "models",
        display: 1,
      },
      {
        title: "Buy Credit",
        icon: "addCredit",
        path: "buyCredit",
        idx: 3,
        level: 1,
        display: 1,
         
      },
      {
        title: "Settings",
        icon: "BsGear",
        idx: 4,
        level: 1,
        path: "",
        display: 1,
        children: [
          {
            title: "Details",
            icon: "|",
            path: "accountDetails",
            idx: 5,
            level: 1,
            display: 1,
          },
          {
            title: "Billing",
            icon: "|",
            path: "billing",
            idx: 6,
            level: 1,
            display: 1,
            permissions: {
              plan: 1,
              invoice: 1,
              editPayment: 1,
            }
          },
          
          {
            title: "Integrations",
            icon: "|",
            path: "integrations",
            idx: 7,
            level: 1,
            display: 1
          },
          // {
          //   title: "Add User",
          //   icon: "|",
          //   path: "addUser",
          //   idx: 7,
          //   level: 1,
          //   display: 0
          // },
          {
            title: "User Management",
            icon: "|",
            path: "userManagement",
            idx: 8,
            level: 1,
            display: 0
          },
          // {
          //   title: "API Doc",
          //   icon: "|",
          //   path: "document",
          //   idx: 8,
          //   level: 1,
          //   display: 1,
          // },
          {
            title: "Industry Profile",
            icon: "|",
            idx: 9,
            level: 1,
            path: "industryProfile",
            display: 1,
          }
        ]
      },
      {
        title: "Contact Us",
        icon: "FiContactUs",
        idx: 10,
        level: 1,
        path: "contactUs",
        display: 1,
      },

    ]
    // if plan is enterprise or subaccount is true 
    // if plan is enterprise and subaccount is true then add user display should be 0 and edit plan should be disabled
    // if plan is enterprise and subaccount is false then add user display should be 1 and edit plan should be enabled
    const plan = cust?.pricing_plan?.plan?.toUpperCase();
    const customerIndustryData = await this.customerIndustryRepository.findOne({ where: { email: cust.email } });
    //if the customer is real estate investor and not subscribed to any plan then hide buy credit module
    if((!customer?.per_unit_price && customerIndustryData?.industry_type===industryTypes.REAL_ESTATE_INVESTORS) || plan==='POSTPAID' ){
      const buyCreditElement = moduleConfigObject.find((element: { path: string; title: string; }) => element.path === 'buyCredit' );
      if (buyCreditElement) {
        buyCreditElement.display = 0;
      }
    }
    if (plan == 'ENTERPRISE' || plan == 'POSTPAID'  || subAccount) {
      // Find the "Settings" element in moduleConfigObj
      const settingsElement = moduleConfigObject.find((element: { path: string; title: string; }) => element.path === '' && element.title === 'Settings');
      if (settingsElement && settingsElement.children) {
        if (subAccount) {
          const billingElement = settingsElement.children.find((child: { path: string; }) => child.path === 'billing');
          if (billingElement) {
            billingElement.display = 0;
          }
        } else {
          // Find the "Add User" element within the "Settings" array and set its display property to 0
          const addUserManagementElement = settingsElement.children.find((child: { path: string; }) => child.path === 'userManagement');
          if (addUserManagementElement) {
            addUserManagementElement.display = 1;
          }
          // enable integrations module
          const integrationsElement = settingsElement.children.find((child: { path: string; }) => child.path === 'integrations');
          if (integrationsElement) {
            integrationsElement.display = 1;
          }
        }
      }
    }
    // if admin then send admin module display as 1
    if (cust.role == 'admin') {
      const adminElement = moduleConfigObject.find((element) => element.title === 'Admin');
      // Check if adminElement is not undefined before accessing its properties
      if (adminElement) {
        adminElement.display = 1;
      }
    }
    
    let displayDetails = {
      name: findbyemail[0]?.name,
      email: findbyemail[0]?.email,
    }
    cust = { module_config_obj: moduleConfigObject, ...cust, display_details: displayDetails }
    try {
      last4Digit = await stripPaymentInfo(cust);
    } catch (error) {
      console.error("Error in stripPaymentInfo function", error)
    }
    
    return { token, cust: { ...cust,add_ons_needOrderId,cc_last4digit: last4Digit?.cardInfo?.last4, brand: last4Digit?.cardInfo?.brand}};
  }

  @authenticate("jwt")
  @get("/logins/count")
  @response(200, {
    description: "Customer model count",
    content: { "application/json": { schema: CountSchema } },
  })
  async count(@param.where(Customer) where?: Where<Customer>): Promise<Count> {
    return this.customerRepository.count(where);
  }

  @authenticate("jwt")
  @get("/logins")
  @response(200, {
    description: "Array of Customer model instances",
    content: {
      "application/json": {
        schema: {
          type: "array",
          items: getModelSchemaRef(Customer, { includeRelations: true }),
        },
      },
    },
  })
  async find(@param.filter(Customer) filter?: Filter<Customer>): Promise<Customer[]> {
    return this.customerRepository.find(filter);
  }

  @authenticate("jwt")
  @patch("/logins")
  @response(200, {
    description: "Login PATCH success count",
    content: { "application/json": { schema: CountSchema } },
  })
  async updateAll(
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(Customer, { partial: true }),
        },
      },
    })
    customer: Customer,
    @param.where(Customer) where?: Where<Customer>
  ): Promise<Count> {
    return this.customerRepository.updateAll(customer, where);
  }

  @authenticate("jwt")
  @get("/apidoc")
  @response(200, {
    description: "CRM integratiopn Api documentation",
  })
  async apidoc(
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(Customer, { partial: true }),
        },
      },
    })
    @param.where(Customer) where?: Where<Customer>
  ): Promise<Object> {
    const apiDocObject = [
        {
          title: "Generate Token",
          endpoint: '/generate-token',
          method: 'POST',
          body: [
            {
                text: JSON.stringify({"api_key": "api-key","email": "abc@gmail.com"}),
                type: 'code',
                },
                {
                    data : [
                        {
                            text: [
                                'api_key will be provided by Reworked.',
                                'Email is the id from which you created a Reworked account.',
                            ],
                            type: 'list',
                        },
                    ],
                    type: 'noncode'
                },
          ],
          response: [
            {
                text: JSON.stringify({"msg": "Token generated successfully","data": {"token": "Auth Token"}}),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'Here token should be send as authorization header in process leads api',
                            'Token will be valid for 24 hours.',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
        },
        {
          title: "Process Leads",
          endpoint: '/process-leads',
          method: 'POST',
          headers: [
            {
                text: JSON.stringify({"Authorization": "token from generate-token api’s response"}),
                type: 'code',
            },
          ],
          body: [
            {
                text: JSON.stringify({"file_url": "file upload file url","email": "email","callback_url": "callback_url"}),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'file_url - here is url of the file to process',
                            'callback_url - send url to be called once fill processing is completed.',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
          response: [
            {
                text: JSON.stringify({
                    "msg": "File processing started",
                    "data":  {
                        "token": "auth token",
                        "file_upload_identifier": "file upload identifier"
                    }
                }),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'token to be used in file status api.',
                            'the token will be valid for 24 hours.',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
        },
        {
          title: "File Status",
          endpoint: '/file-status',
          method: 'POST',
          headers: [
            {
                text: JSON.stringify({
                    "Authorization": "token from /process-leads api’s response"
                }),
                type: 'code',
            },
          ],
          body: [
            {
                text: JSON.stringify({
                    "file_upload_identifier": "file upload identifier",
                    "email": "email"
                }),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'file upload identifier from process leads response.',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
          response: [
            {
                text: JSON.stringify({
                    "msg": "File Processed Successfully",
                    "data": {
                        "processed_file_url": "Process file URL",
                        "status": "PROCESSING"
                    }
                }),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'Status can be  STARTED/PROCESSED/FAILED/PROCESSING',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
        },

        {
          title: "Authenticate User",
          endpoint: '/user-authenticate',
          method: 'POST',
          headers: [],
          body: [
            {
                text: JSON.stringify({
                    "password": "password",
                    "email": "email"
                }),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'Email and password of user.',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
          response: [
            {
                text: JSON.stringify({
                    "msg": "User Authenticated Successfully",
                    "data": {
                      "api_secret_key": "api-secret-key of user"
                    }
                }),
                type: 'code',
            },
            {
                data : [
                    {
                        text: [
                            'in case of error status code will be 403',
                        ],
                        type: 'list',
                    },
                ],
                type: 'noncode'
            },
          ],
        },
      ];
    return apiDocObject
  }

  @authenticate("jwt")
  @get("/logins/{id}")
  @response(200, {
    description: "Customer model instance",
    content: {
      "application/json": {
        schema: getModelSchemaRef(Customer, { includeRelations: true }),
      },
    },
  })
  async findById(
    @param.path.string("id") id: string,
    @param.filter(Customer, { exclude: "where" }) filter?: FilterExcludingWhere<Customer>
  ): Promise<Customer> {
    return this.customerRepository.findById(id, filter);
  }

  @authenticate("jwt")
  @patch("/logins/{id}")
  @response(204, {
    description: "Customer PATCH success",
  })
  async updateById(
    @param.path.string("id") id: string,
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(Customer, { partial: true }),
        },
      },
    })
    customer: Customer
  ): Promise<void> {
    await this.customerRepository.updateById(id, customer);
  }

  @authenticate("jwt")
  @put("/logins/{id}")
  @response(204, {
    description: "Customer PUT success",
  })
  async replaceById(@param.path.string("id") id: string, @requestBody() customer: Customer): Promise<void> {
    await this.customerRepository.replaceById(id, customer);
  }

  @authenticate("jwt")
  @del("/logins/{id}")
  @response(204, {
    description: "Customer DELETE success",
  })
  async deleteById(@param.path.string("id") id: string): Promise<void> {
    await this.customerRepository.deleteById(id);
  }

  // @authenticate('jwt')
  @get("/processedLines")
  @response(200, {
    description: "Total processed lines",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async findProcessedLines(): Promise<{ data: object; msg: String }> {
    // initializing variable for response
    let totalProcessedLines = 0;
    let msg = "Total processed lines fetched successfully";

    // try catch for error handling
    try {
      // converting it as collection
      const fileHistoryRepository = (this.fileHistoryRepository.dataSource.connector as any).collection("FileHistory");

      // aggregate query to find total number of processed lines from file history collection
      const totalHistory = await fileHistoryRepository
        .aggregate([
          {
            $group: {
              _id: "$total",
              totalProcessedLines: {
                $sum: {
                  $toLong: "$record_count",
                },
              },
            },
          },
        ])
        .get();

      // if record found then assigning it to variable
      if (totalHistory && totalHistory.length) totalProcessedLines = totalHistory[0].totalProcessedLines || 0;
    } catch (error) {
      console.error("error in finding total processed lines");
      msg = error.message;
    }
    let totalMailersSaved = Math.floor(totalProcessedLines * 0.4099);
    let totalPaperSaved = Math.floor(totalProcessedLines * 0.0598);
    const response = {
      data: { totalProcessedLines, totalMailersSaved, totalPaperSaved },
      msg,
    };
    return response;
  }

  @authenticate("jwt")
  @post("/customerDetails")
  @response(200, {
    description: "Get Customer data",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async findCustomrDetailss(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["email"],
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
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<Response<any, Record<string, any>>> {
    // initializing variable for response
    let msg = "Customer data fetched successfully";
    let customerEmail = request.headers.email as string;
    let industryName = request.headers.industry as string
    let responseToSend = { data: {}, msg };
    // try catch for error handling
    try {
      let isAdmin: Boolean = false;
      if (request?.body?.email) {
        // check if request is coming from admin
        isAdmin = await CustomerService.isAdmin(request.headers.email as string, this.customerRepository);
        if (isAdmin) {
          customerEmail = request.body.email as string;
        } else {
          responseToSend.msg = "Invalid request - only admin can request info";
          return response.status(400).send(responseToSend);
        }
      }

      //header email is requester, body email is who they are seeking info about
      let ans = await CustomerService.getCustomerDetailsObj(
        customerEmail,
        this.customerRepository,
        this.fileHistoryRepository,
        this.promoRepository,
        this.transactionHistoryRepository,
        this.customerIndustryRepository,
        industryName
      );
      // console.log("ans = ",ans)
      return response.status(ans[0]).send(ans[1]);
    } catch (error) {
      console.error("error in findCustomrDetails", error);
      responseToSend.msg = error.message;
      return response.status(500).send(responseToSend);
    }
  }

  @authenticate("jwt")
  @post("/upsert-row-credits")
  async upsertRowCredits(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ) {
    let ans = await CustomerService.upsertRowCredits(
      request.headers.email as string,
      request.body.email,
      request.body.newTotalRowCredits,
      this.customerRepository,
      this.fileHistoryRepository,
      this.adminEventsRepository,
      this.promoRepository,
      this.transactionHistoryRepository,
      this.customerIndustryRepository
    );
    // console.log("ans[1] = ", ans[1]);
    return response.status(ans[0]).send(ans[1]);
  }
  @authenticate("jwt")
  @post("/upsert-cost-per-row")
  async upsertCostPerRow(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ) {
    let ans = await CustomerService.upsertCostPerRow(
      request.headers.email as string,
      request.body.email,
      request.body.newCostPerRow || 0,
      request.body.leadGenPerUnitPrice || 0,
      this.customerRepository,
      this.fileHistoryRepository,
      this.adminEventsRepository,
      this.promoRepository,
      this.transactionHistoryRepository,
      this.customerIndustryRepository
    );
    // console.log("ans[1] = ", ans[1]);
    return response.status(ans[0]).send(ans[1]);
  }
  @authenticate("jwt")
  @get("/get-per-unit-price")
  async getPerUnitPriceForCustomer(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {
    try {
      const email = request.headers.email as string;
      const response = await CustomerService.getPerUnitPriceForCustomer(email, this.customerRepository, this.customerIndustryRepository);
      return res.status(response.status).send(response);
    } catch (error) {
      console.log(error);
      return res.status(500).send({ msg: error.message });
    }
  }

  @authenticate("jwt")
  @post("/create-industry-customer")
  @response(200, {
    description: "Customer Industry relation model instance",
    content: { "application/json": { schema: getModelSchemaRef(CustomerIndustry) } },
  })
  async createNewIndustryForCustomer(
    @requestBody({
      content: {
        "application/json": {
          schema: getModelSchemaRef(CustomerIndustry, {
            title: "NewCustomerIndustry",
            exclude: ["id", "industry_profile"],
          }),
        },
      },
    }) customerIndustry: Omit<CustomerIndustry, "id">,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {
    try {
      // const email = request.headers.email as string;
      // const industryName = request.headers.industry as string
      const customerIndustryExists = await this.customerIndustryRepository.findOne({ where: { email: customerIndustry.email, industry_type: customerIndustry.industry_type } })
      if (customerIndustryExists) {
        return res.status(200).send({ msg: "Customer with this Industry already exists" });
      } else {
        const newCustomerIndustry = await this.customerIndustryRepository.create(customerIndustry)
        return res.send(newCustomerIndustry);
      }
    } catch (error) {
      console.log(error);
      return res.status(500).send({ msg: error.message });
    }
  }

  @authenticate("jwt")
  @post("/upsert-industry-profile")
  async upsertIndustryProfile(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {
    try {
      const email = request.headers.email as string;
      let industryName = request.headers.industry as string
      let customerUpdatePromise = null;

      const [customerIndustry, customer] = await Promise.all([
        this.customerIndustryRepository.findOne({ where: { email } }),
        this.customerRepository.findOne({ where: { email } })
      ]);

      if (!customerIndustry || !customer) {
        return res.status(404).send({ msg: "Customer with given industry not found" });
      }

        const { id: industryProfileId } = request.body;
        let { industry_profile: industryProfile = [] as IndustryProfile[] } = customerIndustry;
        if (!industryProfile) industryProfile = [];
        if (request.body.default) industryProfile.map((ele: { default: boolean }) => (ele.default = false));
        let index = -1;
        for (let i = 0; i < industryProfile.length; i++) {
          if (industryProfile[i].id == industryProfileId) {
            index = i;
            break;
          }
        }
        industryName = request.body?.question_answers?.industryType || industryTypes.REAL_ESTATE_INVESTORS;
        if (index >= 0) {
          industryProfile[index].question_answers = request.body.question_answers;
          industryProfile[index].name = request.body.name;
          industryProfile[index].default = request.body.default;
        } else {
          delete request.body.id;
          const newIndustryProfile = {
            id: new ObjectID(),
            ...request.body,
          };
          if (industryName == industryTypes.SOLAR_INSTALLER && industryProfile.length == 0) {
            customer.row_credits = 100;
            customerUpdatePromise = this.customerRepository.update(customer);
          }
          if (!(industryProfile && industryProfile.length)) {
            newIndustryProfile.default = true;
        //     customer.industry_type = industryName;
        //     customerUpdatePromise = this.customerRepository.update(customer);
          }
          industryProfile.push(newIndustryProfile);
        }
        customerIndustry.industry_profile = industryProfile;
        customerIndustry.industry_type = industryName;
        const customerIndustryUpdatePromise = this.customerIndustryRepository.updateById(customerIndustry.id, customerIndustry);
        await Promise.all([
          customerIndustryUpdatePromise,
          customerUpdatePromise
        ]);
        
        // email admin if industry type is real estate and data source is Investment dominator 
        // to send ID an api key if user signed up with google oauth
        if(industryName && industryName == industryTypes.REAL_ESTATE_INVESTORS && request.body?.question_answers?.data_source?.toLowerCase() == 'investmentdominator'){
            const optionsforAdminMail = {
                content: `New user ${customer?.email} has signed up with source = investmentdominator. 
                          Please check Auth0 to see if they're a google auth user. 
                          If they are then send the API key to the Investment Dominator guys`
            }
            await sendEmailToAdmin('uploadedFileName', customer, this.customerRepository, optionsforAdminMail);
        }
        const responseToSend = {
          data: {
            industryProfile,
          },
          msg: "Industry Profile updated successfully",
        };
        return res.status(200).send(responseToSend);
      
    } catch (error) {
      console.log(error);
      return res.status(500).send({ msg: error.message });
    }
  }

  @authenticate("jwt")
  @put("/default-industrial-profile")
  async toggleDefaultIndustrialProfileStatus(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["industrial_profile_id"],
            properties: {
              industrial_profile_id: {
                type: "string",
              },
            },
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {
    try {
      const email = request.headers.email as string;
      let industryName = request.headers.industy as string

      const [customerIndustry, customer] = await Promise.all([
        this.customerIndustryRepository.findOne({ where: { email } }),
        this.customerRepository.findOne({ where: { email } })
      ]);

      if (!customerIndustry || !customer) {
        return res.status(404).send({ msg: "Customer with given industry not found" });
      }

        const { industrial_profile_id: industryProfileId } = request.body;
        if (!industryProfileId) return res.status(400).send({ msg: "id of profile not found" })
        let { industry_profile: industryProfile = [] as IndustryProfile[] } = customerIndustry;
        if (!industryProfile) industryProfile = [];
        for (let i = 0; i < industryProfile.length; i++) {
          if (industryProfile[i].id == industryProfileId) {
            const industryProfileObj = industryProfile[i];
            industryProfile[i].default = true;
            industryName = industryProfileObj.question_answers?.industryType || industryTypes.REAL_ESTATE_INVESTORS;
          } else industryProfile[i].default = false;
        }
        
        customerIndustry.industry_profile = industryProfile;
        customerIndustry.industry_type = industryName;
        // customer.industry_type = industryName;
        await Promise.all([
          this.customerIndustryRepository.updateById(customerIndustry.id, customerIndustry),
        //   this.customerRepository.update(customer)
        ]);

        const responseToSend = {
          data: {
            industryProfile,
          },
          msg: "Industry Profile updated successfully",
        };
        return res.status(200).send(responseToSend);
    
    } catch (error) {
      console.log(error);
      return res.status(500).send({ msg: error.message });
    }
  }

  @authenticate("jwt")
  @put("/delete-industrial-profile")
  async deleteIndustrialProfile(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["industrial_profile_id"],
            properties: {
              industrial_profile_id: {
                type: "string",
              },
            },
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {
    try {
      const email = request.headers.email as string;
      const customerIndustry = await this.customerIndustryRepository.findOne({ where: { email: email } })
      if (customerIndustry) {
        const { industrial_profile_id: industryProfileId } = request.body;
        if (!industryProfileId) return res.status(400).send({ msg: "id of profile not found" })
        let { industry_profile: industryProfile = [] as IndustryProfile[] } = customerIndustry;
        if (!industryProfile) industryProfile = [];
        
        industryProfile = industryProfile.filter((profile)=> profile.id != industryProfileId)
        customerIndustry.industry_profile = industryProfile;
        await this.customerIndustryRepository.updateById(customerIndustry.id, customerIndustry)

        const responseToSend = {
          msg: "Industry Profile deleted successfully",
        };
        return res.status(200).send(responseToSend);
      } else {
        return res.status(404).send({ msg: "Customer not have industry profiles" });
      }
    } catch (error) {
      console.log(error);
      return res.status(500).send({ msg: error.message });
    }
  }


  // @authenticate("jwt")
  // @post("/migrate")
  // @response(200, {
  //   description: "Migration Done Successfully",
  //   content: { "application/json": { schema: { msg: "Migration Completed" } } },
  // })
  // async migrate(@requestBody({
  //   content: {
  //     "application/json": {
  //       schema: {
  //         type: "object",
  //         properties: {
  //           deleteFromCustomer: {
  //             type: "boolean",
  //           },
  //         },
  //       },
  //     },
  //   },
  // })
  // @inject(RestBindings.Http.REQUEST)
  // request: Request) {
  //   const customerCollection = (this.customerRepository.dataSource.connector as any).collection('Customer')
  //   const newCustomers: [Customer] = await customerCollection.aggregate([
  //     {
  //       '$lookup': {
  //         'from': 'CustomerIndustry',
  //         'localField': 'email',
  //         'foreignField': 'email',
  //         'as': 'result'
  //       }
  //     }, {
  //       '$match': {
  //         'result': {
  //           '$size': 0
  //         }
  //       }
  //     }
  //   ]).toArray()
  //   const toCreateCustomerSchema = []
  //   for (let i = 0; i < newCustomers.length; i++) {
  //     const currentCustomer = newCustomers[i]
  //     const editCustomer = currentCustomer.investment_profile?.filter((a: IndustryProfile) => {
  //       if (!a.question_answers) return false
  //       if (Array.isArray(a.question_answers)) return false
  //       return true
  //     })
  //     const newCustomer = {
  //       email: newCustomers[i].email,
  //       industry_type: industryTypes.REAL_ESTATE_INVESTORS,
  //       industry_profile: editCustomer && editCustomer.length ? editCustomer : []
  //     }
  //     toCreateCustomerSchema.push(newCustomer)
  //   }
  //   if (request.body.deleteFromCustomer) {
  //     const response = await customerCollection.updateMany({ investment_profile: { $exists: true } }, { $unset: { investment_profile: 1 } })
  //   }
  //   const res = await this.customerIndustryRepository.createAll(toCreateCustomerSchema)
  //   return res
  // }


  @authenticate('jwt')
  @get("/insights")
  @response(200, {
    description: "User level insights",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async generateInsights(
    @inject(RestBindings.Http.REQUEST)
    request: Request,
  ): Promise<{ data: object; msg: string }> {
    const email = request.headers.email as string || '';

    try {
      // Get references to the collections

      const fileHistoryRepository = (this.fileHistoryRepository.dataSource.connector as any).collection("FileHistory");
      const customerModelsRepository = (this.customerModelsRepository.dataSource.connector as any).collection("CustomerModels");
      const generateLeadsRepository = (this.generateLeadsRepository.dataSource.connector as any).collection("GenerateLeadsModel");
      const customerIndustryPromise = this.customerIndustryRepository.findOne({
        where: {
          email
        }
      })
      // Aggregation pipelines for both queries
      const totalHistoryPipeline = [
        {
          $match: { email: email },
        },
        {
          $group: {
            _id: "$email",
            totalProcessedLines: { $sum: { $toLong: "$record_count" } },
            totalMailersSaved: { $sum: { $toLong: "$rows_below_100" } },
          },
        },
      ];

      const totalModelPipeline = [
        {
          $match: { email: email },
        },
        {
          $group: {
            _id: "$email",
            totalModels: { $sum: 1 },
          },
        },
      ];
      const newContactFoundPipeline = [
        {
          $match: { email: email, status: 2 },
        },
        {
          $group: {
            _id: "$email",
            newContactsFound: { $sum: { $toLong: "$lead_count" } },
          },
        },
      ];
      // Fetch data from both aggregations concurrently
      const [totalHistory, totalModelHistory, totalNewContactFound, customerIndustryData] = await Promise.all([
        fileHistoryRepository.aggregate(totalHistoryPipeline).get(),
        customerModelsRepository.aggregate(totalModelPipeline).get(),
        generateLeadsRepository.aggregate(newContactFoundPipeline).get(),
        customerIndustryPromise,
      ]);

      // Extract necessary information from the results
      const totalProcessedLines = totalHistory?.[0]?.totalProcessedLines || 0;
      const totalMailersSaved = totalHistory?.[0]?.totalMailersSaved || 0;
      const totalModelsBuilt = totalModelHistory?.[0]?.totalModels || 0;
      const newContactsFound = totalNewContactFound?.[0]?.newContactsFound || 0; //TODO to calculate this somehow

      let savingAmountFactor = industrTypesMetaData[customerIndustryData?.industry_type || '']?.savings_calculation_factor || 0.67;
      // Calculate mailer savings in dollars
      const totalSavedInDollars = Math.floor(totalMailersSaved * savingAmountFactor);

      // Prepare the response object
      const response = {
        data: {
          totalProcessedLines,
          totalMailersSaved,
          totalSavedInDollars,
          totalModelsBuilt,
          newContactsFound,
        },
        msg: "Insights generated successfully",
      };

      // Return the response
      return response;
    } catch (error) {
      // Handle errors and provide appropriate response
      console.error("Error in finding insights:", error);
      return {
        data: {},
        msg: error.message,
      };
    }
  }


  @authenticate("jwt")
  @get("/investment-profile")
  @response(200, {
    description: "Get Investment Profile",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async getInvestmentProfile(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<Response<any, Record<string, any>>> {
    // initializing variable for response
    let msg = "Investment Profile data fetched successfully";
    let customerEmail = request.headers.email as string;
    let responseToSend = { data: {}, msg };
    let status = 200;
    let findCustomerByEmail
    // try catch for error handling
    try {
      if (customerEmail) {
        let industryName = industryTypes.REAL_ESTATE_INVESTORS;

        const customerCollection = (this.customerRepository.dataSource.connector as any).collection("Customer");
        const findCustomerByEmailAggregate = await customerCollection
          .aggregate([
            {
              $match: {
                email: customerEmail,
              },
            },
            {
              $project: {
                email: 1,
                name: 1,
                industry_type: 1,
              },
            },
            {
              $lookup: {
                from: "CustomerIndustry",
                let: {
                  email: "$email",
                },
                pipeline: [
                  {
                    $match: {
                      $expr: {
                        $and: [
                          {
                            $eq: ["$email", "$$email"],
                          },
                          {
                            $eq: ["$industry_type", industryName],
                          },
                        ],
                      },
                    },
                  },
                  {
                    $project: {
                      _id: 0,
                      email: 0,
                    },
                  },
                ],
                as: "industry_info",
              },
            },
            {
              $unwind: {
                path: "$industry_info",
                preserveNullAndEmptyArrays: true,
              },
            },
          ])
          .toArray();
        findCustomerByEmail = findCustomerByEmailAggregate[0] ? findCustomerByEmailAggregate[0] : null;
        responseToSend.data = findCustomerByEmail;
      } else {
        responseToSend.msg = "Email id Missing";
        status = 500;
      }

      return response.status(status).send(responseToSend);
    } catch (error) {
      console.error("error in findInvestment profile", error);
      responseToSend.msg = error.message;
      return response.status(500).send(responseToSend);
    }
  }

  @authenticate("jwt")
  @post("/regenerate-postpaid-invoice")
  @response(200, {
    description: "Customer Industry relation model instance",
    content: { "application/json": { schema: getModelSchemaRef(CustomerIndustry) } },
  })
  async regeneratePostpaidInvoice(
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
              transaction_id: {
                type: "string",
              },
            },
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {
    try {

        let msg = "";
        let data = {};
        let statusCode = 200;
        const transaction_id = request.body.transaction_id
        const email = request.body.email

        if (!transaction_id) {
            statusCode = 400;
            msg = "transition_id are missing";
            return res.status(statusCode).send({
                data: null,
                msg
            });
        } 
        if (!email) {
            statusCode = 400;
            msg = "email are missing";
            return res.status(statusCode).send({
                data: null,
                msg
            });
        } 


        const filter: Where<Customer> = {
          $or: [
              { 'pricing_plan.plan': 'postpaid' }, // Case-insensitive match
              { 'pricing_plan.plan': 'POSTPAID' } // Exact match
            ],
            'email': email,
          } as Where<Customer>;
          
        let customerData = await this.customerRepository.findOne({ where: filter })

        let failedTransactionData = await this.transactionHistoryRepository.findOne({ where: { id: transaction_id } })
        let totalAmount = failedTransactionData!.meta_data.total_cost
        let metaData = failedTransactionData!.meta_data
        const noOfCredits = failedTransactionData!.meta_data.no_of_credits
        const sourceType = failedTransactionData!.meta_data.source_type

        if (totalAmount === "NAN") {
            const minusMonth = 1; // to fetch dates for last month
            // const { startDate, endDate } = getFirstAndLastDateOfMonth(minusMonth);
            // let totalCreditsOfLastMonth = await creditsUsedForDates(email, this.fileHistoryRepository, startDate, endDate);
            let totalCreditsOfLastMonth = noOfCredits;
            if (totalCreditsOfLastMonth) {
                // totalCreditsOfLastMonth = 1000;
                const responseFromFunction = await CustomerService.getPerUnitPriceForCustomer(email, this.customerRepository, this.customerIndustryRepository);
                if (customerData!.row_credits >= totalCreditsOfLastMonth) {
                    const responseMsg = `Customer have ${customerData!.row_credits} and ${totalCreditsOfLastMonth} used this month, now ${customerData!.row_credits -= totalCreditsOfLastMonth} rows left.`;
                    customerData!.row_credits -= totalCreditsOfLastMonth;
                    // terminate
                    const updatedTransactionHistory = new TransactionHistory({
                        ...failedTransactionData,
                        invoice_amount: 0,
                        email,
                        meta_data: {...metaData, total_cost: 0},
                        error: false,
                        error_detail: responseMsg,
                    });
                    this.transactionHistoryRepository.updateById(failedTransactionData?.id, updatedTransactionHistory);
                    await this.customerRepository.update(customerData!);
                    return res.status(200).send({ data: null, msg: responseMsg });
                } else {
                    if (responseFromFunction?.data?.perUnitCostAndRange && responseFromFunction?.data?.perUnitCostAndRange.length) {
                        totalCreditsOfLastMonth = totalCreditsOfLastMonth - customerData!.row_credits;
                        customerData!.row_credits = 0;
                        let totalAmountFromRange = await getPriceFromRange(totalCreditsOfLastMonth, responseFromFunction.data.perUnitCostAndRange);
                        totalAmount = totalAmountFromRange
                        metaData.total_cost = totalAmountFromRange
                    } else {
                        return res.status(402).send({ msg: "perUnitCostAndRange is not define for customer." });
                    }
                  }               
            }
            // await this.customerRepository.update(customerData!);
        }

        const args = {
            totalAmount, metaData, noOfCredits, email, payment_type: sourceType,
            invoiceItemDescription: 'Post Paid Billing', leadsToAdd: false,
            sourceType, failedTransactionData
          };

        const responseFromStripe = await stripePayment(args, customerData, this.customerRepository, this.transactionHistoryRepository, this.adminEventsRepository)
        const dataFromStripe: any = responseFromStripe.data;

        msg = responseFromStripe.msg;
        statusCode = responseFromStripe.statusCode;
        let optionsforAdminMail = null;
        if (statusCode == 200) {
            optionsforAdminMail = {
                content: `Invoice created for User ${customerData!.email} and invoice is ${dataFromStripe.invoice_pdf}`
            }
        } else {
            optionsforAdminMail = {
                content: `Invoice failed for User ${customerData!.email} and total amount was ${totalAmount}, reason: ${msg}`
            }
        }
        
        await sendEmailToAdmin('modelFilename', customerData!, this.customerRepository, optionsforAdminMail)

        await this.customerRepository.update(customerData!);
        
        return res.status(statusCode).send({
            data: dataFromStripe,
            msg
        });

    } catch (error) {
      console.log(error);
      return res.status(500).send({ msg: error.message });
    }
  }


  // @authenticate("jwt")
  @post("/zapier-authentication")
  @response(200, {
    description: "Get Customer data",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async zapierAuthentication(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: [""],
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
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<Response<any, Record<string, any>>> {
    console.log(`${new Date(), "Authentication Api called by Zapier"}`)
    let customerApiKey = request.headers["x-api-key"] as string;
    // try catch for error handling
    try {

      if(customerApiKey) {
        const customerData = await this.customerRepository.findOne({
          where: {
            api_secret_key: customerApiKey
          }
        })
        // console.log("customerApiKeycustomerData",  customerApiKey, customerData);
        if(customerData){
          return response.status(200).send({ msg: "Api key validated successfully" });    
        }else {
          return response.status(400).send({ key: "Api key invalid" });
        }
      }
      else {
        return response.status(400).send({ msg: "Header missing" });  
      }
      
    } catch (error) {
      console.error("error in findCustomrDetails", error);
      return response.status(500).send({data: null, msg: error.message});
    }
  }


  // @authenticate("jwt")
  @post("/zapier-process-file")
  @response(200, {
    description: "Get Customer data",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async processZapierFile(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: [""],
            properties: {
              file_url: {
                type: "string",
              },
            },
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<Response<any, Record<string, any>>> {
    // try catch for error handling
    try {
      const file_url = `https://drive.google.com/uc?export=download&id=${request.body.file_url}`;
      console.log("logged in trigger by zapier", file_url)
      const webhookUrl = request.url;

      // Use URLSearchParams to extract the value of 'api_key'
      const apiKey = new URLSearchParams(webhookUrl.split('?')[1]).get('api_key') || '';
      const customerData = await this.customerRepository.findOne({
        fields: { login_history: false, file_history: false },
        where: {
          api_secret_key: apiKey,
        }
      })
      
      if (customerData) {
        const integrationsData = await this.integrationsRepository.findOne({
          where: {
            email: customerData.email,
            platform: platformIntegrations.GOOGLEDRIVE
          }
        })
        const payload = {
          crmKey: apiKey,
          email: customerData.email
        };
        const crmSecretKey = process.env.CRM_API_KEY;
        const token = jwt.sign(payload, crmSecretKey, { expiresIn: '48h' });
        const base_url = `${process.env.BASE_URL}/api/process-leads`

        const process_leads_payload = {
          "file_url": file_url,
          "email": customerData.email,
          "overwrite": true, 
          "callback_url": integrationsData?.metadata?.destination_address,
          "platform": platformIntegrations.GOOGLEDRIVE
        }
        const headers = {
          'Authorization': token,
          'Content-Type': 'application/json'
        }

        try {
          console.log("Step 2: Processing leads...");
          const processLeadsResponse = axios.post(base_url, process_leads_payload, { headers }).then((data) => {
            console.log(data);
          });
        } catch (error) {
          console.error("Error processing leads:", error);
        }
        // return response.status(200).send({ id: new Date() });
        return response.status(200).send({ msg: 'File processing started' });

      } else {
        return response.status(404).send({ msg: 'Customer data not found', data: null });
      }

    } catch (error) {
      console.error("error in findCustomrDetails", error);
      return response.status(500).send({data: null, msg: error.message});
    }
  }

  // Update customer survey answer
  @authenticate('jwt')
  @post('/survey-answer')
  @response(200, {
    description: 'Update customer survey answer',
    content: {
      'application/json': {
        schema: {
          type: 'object',
          properties: {
            msg: { type: 'string' },
          },
        },
      },
    },
  })
  async updateSurveyAnswer(
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'object',
            required: ['answered', 'platform'],
            properties: {
              answered: {
                type: 'boolean',
              },
              platform: {
                type: 'string',
              },
            },
          },
        },
      },
    })
    survey_data: { answered: boolean; platform: string },
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any, Record<string, any>>> {
    try {
      const authHeader: any = request.headers.authorization;
      const authenticateToken = authHeader.replace('Bearer ', '');
      const decodedToken: any = jwt.decode(authenticateToken);
      const email = decodedToken?.email;

      if (!email) {
        return response.status(500).send({ msg: 'Email is required.' });
      }
      const customerDetails = await this.customerRepository.findOne({ where: { email } });

      if (!customerDetails) {
        throw new Error('Customer not found');
      }

      // Update the customer record with surveyData
      await this.customerRepository.updateById(customerDetails.id, {
        survey_answer: survey_data,
      });
      return response.status(200).send({ msg: 'Survey answer updated successfully.' });
    } catch (error) {
      console.error('Error updating survey answer:', error);
      return response.status(500).send({ msg: 'Failed to update survey answer.' });
    }
  }

// get all integrations
  @authenticate("jwt")
  @get("/integrations")
  @response(200, {
    description: "Customer model instance",
    content: {
      "application/json": {
        schema: getModelSchemaRef(Customer, { includeRelations: true }),
      },
    },
  })
  async getAllIntegrations(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any>> {
    try{
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
      const integrations = await this.integrationsRepository.find({
        where: {
          email
        }
      });
      const getLeadsScoredForPlatform = async (platform: string): Promise<number> => {
        const records = await this.fileHistoryRepository.find({ where: { email } });
        return records
          .filter((file: any) => file?.source === platform)
          .reduce((total, record) => total + (Number(record.record_count) || 0), 0);
      };
  
      const filteredIntegrations = await Promise.all(
        integrations.map(async (integration: any) => {
          const leadsScored = await getLeadsScoredForPlatform(integration.platform);
          return {
            platform: integration.platform,
            code: integration.code,
            access_token_expires_at: integration.access_token_expires_at,
            token_type: integration.token_type,
            column_mapping: integration.column_mapping,
            created_at: integration.created_at,
            updated_at: integration.updated_at,
            leadsScored,
            metadata: integration.metadata || {}
          }
        })
      )
      return response.status(200).send({
        msg: "List fetched successfully",
        data: filteredIntegrations
      })
    }catch(error){
      console.log("Error in integrations api of customer", error.message);
      return response.status(500).send({ msg: 'Error fetching integrations', date: {details: error.message }});
    }
  };

}
