import {
    get,
    post,
    requestBody,
    RestBindings,
    Response,
    Request,
  } from '@loopback/rest';
  import { inject } from "@loopback/core";
  import axios from 'axios';
  import {
    CustomerModelsRepository, CustomerRepository, FileHistoryRepository, GenerateLeadsRepository, CustomerIndustryRepository,
    TransactionHistoryRepository, AdminEventsRepository, IntegrationsRepository
  } from "../repositories";
  import { repository } from '@loopback/repository';
  import { platformIntegrations } from '../constant/platform_integrations';
  import { authenticate } from '@loopback/authentication';
  import { extractDetailsFromAuthToken } from '../helper/utils';
  import { sendEmailToAdmin } from "../helper";
  import path from "path";
  import { FileHistory } from '../models';
  const createCsvWriter = require('csv-writer').createObjectCsvWriter;
  import { CRMIntegrationService } from '../services';
  import { ZohoService } from '../services/zoho.service';
import { disconnectIntegration, getClientRegion, UpdateColumnMapping } from '../helper/integrations-helper';
   
  const baseUrl = '/zoho';

  
  export class ZohoController {
    constructor(
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
    ) { }
  
    // OAuth Callback for Zoho authentication
    @authenticate('jwt')
    @get('/zoho-authentication', {
      responses: {
        '200': {
          description: 'OAuth Callback for Zoho',
          content: {
            'application/json': {
              schema: { type: 'object' },
            },
          },
        },
      },
    })
    async handleOAuthCallback(
      @inject(RestBindings.Http.REQUEST) request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
      try {
        const code = request.query.code as string;
        //for testing on dev
        const region:string = 'in';
        const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
        let tokens: any = {};
  
        if (!code) {
          return response.status(400).send({ msg: 'Authorization code is missing' });
        }
        if (!email) {
          return response.status(400).send({ msg: 'Authorization email is missing' });
        }
  
        const customerData = await this.customerRepository.findOne({ where: { email } });
        if (!customerData) {
          return response.status(400).send({ msg: 'Issue with Token' });
        }
  
        const optionsforAdminMail = {
          content: `User ${email} wants to connect with Zoho. 
                    Please check if they are successfully connected with Zoho in the integrations table. 
                    If not, please check logs and inform the dev team. Thank you.`
        };
        sendEmailToAdmin('uploadedFileName', customerData, this.customerRepository, optionsforAdminMail);
  
        // Prepare request to exchange authorization code for access token
        const params = new URLSearchParams();
        params.append('grant_type', 'authorization_code');
        params.append('client_id', process.env.ZOHO_CLIENT_ID as string);
        params.append('client_secret', process.env.ZOHO_CLIENT_SECRET as string);
        params.append('redirect_uri', process.env.ZOHO_REDIRECT_URI as string);
        params.append('code', code);
        // let {region} = await getClientRegion();
        console.log('region = ',region)
        let zohoAuthURL ;
        if(region!='us'){
          const zohoAuthURLArray = process.env.ZOHO_AUTH_URL?.split('.') as string[] ;
          zohoAuthURLArray[2]=region;
          zohoAuthURL = zohoAuthURLArray.join('.')
        }
        else{
          zohoAuthURL = process.env.ZOHO_AUTH_URL as string;
        }
        console.log('cliend id= ',process.env.ZOHO_CLIENT_ID)
        console.log('cliend secret= ',process.env.ZOHO_CLIENT_SECRET)
        console.log('redirect uri= ',process.env.ZOHO_REDIRECT_URI)
        console.log(zohoAuthURL);
         const tokenResponse = await axios.post(
          `${zohoAuthURL}/oauth/v2/token`,
          params.toString(),
          { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } },
        );
        const records = await this.fileHistoryRepository.find({ where: { email } });
        const leadsScored = records.filter((file: any) => file?.source == platformIntegrations.ZOHO).reduce((total, record) => {
          return total + (Number(record.record_count) || 0);
        }, 0);
        // Store tokens in the database
        const columnMapping = {
          [platformIntegrations.ZOHO]: {
          First_Name: 'owner_first_name',
          Last_Name: 'owner_last_name',
          Mailing_Street: 'mail_street_address',
          Mailing_City: 'mail_city',
          Mailing_State: 'mail_state',
          Mailing_Zip: 'mail_zip_code',
           }
        };
        if (tokenResponse) {
          tokens = { ...tokenResponse.data, code };
          const currentTimestamp = Math.floor(Date.now() / 1000); // Timestamp in seconds
          const expireDate = new Date((currentTimestamp + tokens.expires_in) * 1000);
  
          const existingRecord = await this.integrationsRepository.findOne({
            where: { email, platform: platformIntegrations.ZOHO }
          });
         
          
  
          const integrationData = {
            email,
            code,
            platform: platformIntegrations.ZOHO,
            access_token: tokens.access_token,
            access_token_expires_at: expireDate,
            refresh_token: tokens.refresh_token,
            token_type: tokens.token_type,
            column_mapping: columnMapping,
           };
  
          if (existingRecord) {
            await this.integrationsRepository.updateById(existingRecord.id, integrationData);
          } else {
            await this.integrationsRepository.create(integrationData);
          }
        }
  
        return response.status(200).send({ msg: 'Authenticated Successfully', data: { ...tokens, platform: platformIntegrations.ZOHO,leadsScored ,column_mapping:columnMapping} });
      } catch (error) {
        console.error('Error exchanging token:', error.message);
        return response.status(500).send({ msg: 'Error exchanging token', data: { details: error.message } });
      }
    }
    
    @authenticate('jwt')
    @post(`${baseUrl}/import`, {
      responses: {
        '200': {
          description: 'Successful response with fetched contacts from Zoho',
          content: {
            'application/json': {
              schema: {
                type: 'object',
                properties: {
                  results: {
                    type: 'array',
                    items: {
                      type: 'object',
                      properties: {
                        firstName: { type: 'string' },
                        lastName: { type: 'string' },
                        bettyScore: { type: 'string' }, // Adjust type based on expected data
                      },
                    },
                  },
                },
              },
            },
          },
        },
      },
    })
    async importZohoContacts(
      @inject(RestBindings.Http.REQUEST) request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
      @requestBody({
        description: 'The import request details',
        required: true,
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                list_id: { type: 'string', description: 'Zoho list ID' },
              },
              required: ['list_id'],
            },
          },
        },
      })
      body: { list_id: string }
    ): Promise<Response<any>> {
      try {
        const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
        const integrationRecord = await this.integrationsRepository.findOne({
          where: { email, platform: platformIntegrations.ZOHO }
        });
        
        if (!integrationRecord) return response.status(400).send({ msg: 'Integration not found' });
    
        const customerData = await this.customerRepository.findOne({ where: { email } });
        const existingColumnMapping: any = integrationRecord.column_mapping || {};
        const zohoMappings = existingColumnMapping?.[platformIntegrations.ZOHO] || {};
        
        const paramsArray = Object.keys(zohoMappings);
        paramsArray.push('Betty_Score');
        
        let results = [];
        let nextPageToken: string | null = null; // Initialize nextPageToken
        let csvHeader=[];
        csvHeader.push({ id: 'contactId', title: 'contactId' })
        for (const [zohoColumn, zohoProperty] of Object.entries(zohoMappings) as [string,string][]) {
         csvHeader.push({ id: zohoProperty, title: zohoProperty  });  
       }
       do {
        let params = paramsArray.join(',') + `&cvid=${body.list_id}&per_page=200`;  
        if (nextPageToken) {
          params += `&page_token=${nextPageToken}`; // Append the next page token if it exists
        }
        
        // Fetch contacts from Zoho with the current params
        let { status, data, msg } = await ZohoService.zohoApiCall(
          this.integrationsRepository,
          integrationRecord,
          { type: ZohoService.apiUrlsConstant.FetchContacts.value, params }
        );
        if(status===204){
          return response.status(200).send({ data: [], msg: "No contacts to sync" });
        }
        if (status !== 200 || !data) {
          console.error('Error fetching contacts from Zoho:', msg);
          return response.status(500).send({ error: 'Error fetching Zoho contacts', msg });
        }
      
        const contacts = data.data;
        for (const contact of contacts) {
          const contactResult: any = { contactId: contact.id || '' };
      
          for (const [zohoColumn, zohoProperty] of Object.entries(zohoMappings) as [string, string][]) {
            const value = contact[zohoColumn] || '';
            contactResult[zohoProperty] = value;             
          }
          results.push(contactResult);  
        }
      
        // Update the nextPageToken based on the response
        nextPageToken = data.info.next_page_token;
      
          // Break the loop if next_page_token is null
          if (!nextPageToken) {
            break;
          }
      
        } while (true); 
        if (results.length === 0) {
          return response.status(200).send({ data: [], msg: "No contacts to sync" });
        }
    
        const filename = `zoho_${Date.now()}.csv`;
        const jsonFilePath = path.join(__dirname, `../../.sandbox/${filename}`);
        const csvWriter = createCsvWriter({
          path: jsonFilePath,
          header: csvHeader
        });
    
        await csvWriter.writeRecords(results);
        console.log('CSV file was written successfully');
    
        let fileHistoryObj = new FileHistory({
          email,
          filename: filename,
          file_extension: 'csv',
          upload_date: new Date(),
          record_count: results.length,
          status: 2,
          source: platformIntegrations.ZOHO,  
          error_detail: '',
          error: '',
        });
    
        fileHistoryObj = await this.fileHistoryRepository.create(fileHistoryObj);
    
        const options = { filename };
    
        CRMIntegrationService.processFile(
          options,
          customerData,
          fileHistoryObj,
          this.customerRepository,
          this.fileHistoryRepository,
          this.customerIndustryRepository,
          this.transactionHistoryRepository,
          this.adminEventsRepository,
          this.integrationsRepository,
          this.generateLeadsRepository,
        );
    
        return response.status(200).send({
          results,
        });
    
      } catch (error) {
        console.error(error);
        return response.status(500).send({ error: 'Error fetching Zoho contacts', details: error.message });
      }
    }
    @authenticate('jwt')
    @get(`${baseUrl}/disconnect`, {
      responses: {
        '200': {
          description: 'Disconnect zoho',
          content: {
            'application/json': {
              schema: { type: 'object' },
            },
          },
        },
      },
    })
    async disconnectZoho(
      @inject(RestBindings.Http.REQUEST) request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
        const responseFromFunction = await disconnectIntegration(this.integrationsRepository, request?.headers?.authorization || '', platformIntegrations.ZOHO);
        return response.status(responseFromFunction.status).send({ msg: responseFromFunction.msg, data: responseFromFunction.data });
    }

  @authenticate('jwt')
  @get(`${baseUrl}/properties`, {
    responses: {
      '200': {
        description: 'Zoho Contact Property Names',
        content: {
          'application/json': {
            schema: {
              type: 'array',
              items: {
                type: 'string',
              },
            },
          },
        },
      },
    },
  })
  async getContactPropertyNames(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any>> {
    try {
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
      const integrationsData = await this.integrationsRepository.findOne({
        where: { email, platform: platformIntegrations.ZOHO },
      });

      if (!integrationsData) {
        return response.status(400).send({ msg: 'No integration found for the given email' });
      }

      const { status, data, msg } = await ZohoService.zohoApiCall(
        this.integrationsRepository,
        integrationsData,
        { type: ZohoService.apiUrlsConstant.GetAllContactProperties.value },
      );

      const propertyNames = data.fields.map((field:any) =>  field.api_name);

      if (status !== 200) {
        return response.status(status).send({ msg: `Error fetching properties: ${msg}` });
      }

     return response.status(200).send({data:propertyNames , msg:"Properties fetched successfully"});

    } catch (error) {
      console.error('Error fetching properties from Zoho:', error.message || error);
      return response.status(500).send({ msg: 'Failed to fetch properties from Zoho', error: error.message });
    }
  }
  @authenticate('jwt')
  @post(`${baseUrl}/update-column-mappings`, {
    responses: {
      '200': {
        description: 'Column mappings updated successfully',
        content: { 'application/json': { schema: { type: 'object' } } },
      },
      '400': {
        description: 'Bad request',
        content: { 'application/json': { schema: { type: 'object' } } },
      },
    },
  })
  async updateColumnMappings(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
    @requestBody({
      description: 'Updated column mappings',
      required: true,
      content: {
        'application/json': {
          schema: {
            type: 'object',
            properties: {
              updated_column_mappings: { type: 'object', description: 'Updated column mappings', additionalProperties: true },
            },
            required: ['updated_column_mappings'],
          },
        },
      },
    })
    body: { updated_column_mappings: object }
  ): Promise<Response<any>> {
    const responseFromFunction = await UpdateColumnMapping(this.integrationsRepository, request?.headers?.authorization || '', platformIntegrations.ZOHO, body.updated_column_mappings);
    return response.status(responseFromFunction.status).send({ msg: responseFromFunction.msg, data: responseFromFunction.data });
  }
  @authenticate('jwt')
  @get(`${baseUrl}/filtered-list`, {
    responses: {
      '200': {
        description: 'Get filtered list from Zoho',
        content: { 'application/json': { schema: { type: 'object' } } },
      },
    },
  })
  async filteredList(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any>> {

    let filteredLists: any = [];
    const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
  
    const integrationsData = await this.integrationsRepository.findOne({
      where: { email, platform: platformIntegrations.ZOHO },
    });

    // Handle missing integration data
    if (!integrationsData) {
      return response.status(400).send({ msg: 'Issue with Integration' });
    }
    // fetch the filtered lists from Zoho
    let { status, data, msg } = await ZohoService.zohoApiCall(
      this.integrationsRepository,
      integrationsData,
      { type: ZohoService.apiUrlsConstant.GetFilteredLists.value }
    );
    if (status === 200) {
      // Extract only `display_value` and `id` for each custom view
      filteredLists = data.custom_views.map((view: any) => ({
        name: view.display_value,
        listId: view.id,
      }));
    }
    
    return response.status(status).send({
      msg,
      data: filteredLists,
    });
  }

}
  