import {
  get,
  post,
  requestBody,
  RestBindings,
} from '@loopback/rest';
import { inject } from "@loopback/core";
import axios from 'axios';
import { Request, Response } from '@loopback/rest';
import {
  CustomerModelsRepository, CustomerRepository, FileHistoryRepository, GenerateLeadsRepository, CustomerIndustryRepository,
  TransactionHistoryRepository, AdminEventsRepository, IntegrationsRepository
} from "../repositories";
import { repository } from '@loopback/repository';
import { platformIntegrations } from '../constant/platform_integrations';
import { authenticate } from '@loopback/authentication';
import { CRMIntegrationService, HubspotService } from '../services';
import { extractDetailsFromAuthToken } from '../helper/utils';
const createCsvWriter = require('csv-writer').createObjectCsvWriter;
import {  sendEmailToAdmin } from "../helper";
import path from "path";
import { FileHistory } from '../models';
import { disconnectIntegration, UpdateColumnMapping } from '../helper/integrations-helper';
const baseUrl = '/hubspot';

export class HubSpotController {
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
  // OAuth Callback for HubSpot authentication
  @authenticate('jwt')
  @get('/hubspot-authentication', {
    responses: {
      '200': {
        description: 'OAuth Callback for HubSpot',
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
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
      let tokens: any = {};
      if (!code) {
        return response.status(400).send({ msg: 'Authorization code is missing' });
      }
      if (!email) {
        return response.status(400).send({ msg: 'Authorization email is missing' });
      }

      const customerData = await this.customerRepository.findOne({where: {email}})
      if (!customerData) {
        return response.status(400).send({ msg: 'Issue with Token' });
      }
      const optionsforAdminMail = {
        content: `User ${email} wants to connect with Hubspot. 
                  Please check if he is successfully connected with hubspot in integrations table. 
                  If no pls check logs and inform dev team. Thank You.`
      }
      sendEmailToAdmin('uploadedFileName', customerData, this.customerRepository, optionsforAdminMail);
      // Prepare request to exchange authorization code for access token
      const params = new URLSearchParams();
      params.append('grant_type', 'authorization_code');
      params.append('client_id', process.env.HUBSPOT_CLIENT_ID as string);
      params.append('client_secret', process.env.HUBSPOT_SECRET as string);
      params.append('redirect_uri', process.env.HUBSPOT_REDIRECT_URI as string);
      params.append('code', code);
      const tokenResponse = await axios.post(
        `${process.env.HUBSPOT_API_BASE_URL}/oauth/v1/token`,
        params.toString(),
        { headers: { 'Content-Type': 'application/x-www-form-urlencoded' } },
      );
      const records = await this.fileHistoryRepository.find({ where: { email } });
      const leadsScored = records.filter((file: any) => file?.source == platformIntegrations.ZOHO).reduce((total, record) => {
        return total + (Number(record.record_count) || 0);
      }, 0);
      const columnMapping = {
        [platformIntegrations.HUBSPOT]:{
          firstname: 'owner_first_name',
          lastname: 'owner_last_name',
          address: 'mail_street_address',
          city: 'mail_city',
          state: 'mail_state',
          zip: 'mail_zip_code',
          hs_state_code: 'mail_state_name_short_code',
        }
      };
      // store in db
      if (tokenResponse) {
        tokens = { ...tokenResponse.data, code };
        // Current timestamp in seconds
        let currentTimestamp = Math.floor(Date.now() / 1000); // Timestamp in seconds
        let expireDate = new Date((currentTimestamp + tokens.expires_in) * 1000);

        const existingRecord = await this.integrationsRepository.findOne({
          where: { email, platform: platformIntegrations.HUBSPOT }
        });    
       

        const integrationData = {
          email,
          code,
          platform: platformIntegrations.HUBSPOT,
          access_token: tokens?.access_token,
          access_token_expires_at: expireDate,
          refresh_token: tokens?.refresh_token,
          token_type: tokens?.token_type,
          column_mapping: columnMapping
        };

        if (existingRecord) {
          await this.integrationsRepository.updateById(existingRecord.id, integrationData);
        } else {
          await this.integrationsRepository.create(integrationData);
        }
      }
      return response.status(200).send({ msg: 'Authenticated Successfully', data: { ...tokens, platform: platformIntegrations.HUBSPOT,leadsScored,column_Mapping:columnMapping } });
      // return response.status(200).send(tokens);
    } catch (error) {
      console.error('Error exchanging token:', error.message);
      return response.status(500).send({ msg: 'Error exchanging token', data: { details: error.message } });
    }
  }

  // Start Export - HubSpot Contacts
  @authenticate('jwt')
  @post(`${baseUrl}/import`, {
    responses: {
      '200': {
        description: 'Import contacts',
        content: { 'application/json': { schema: { type: 'object' } } },
      },
    },
  })
  async importContacts(
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
              list_id: { type: 'string', description: 'HubSpot list ID' },
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
      const listId = body.list_id;
      const integrationRecord = await this.integrationsRepository.findOne({
        where: { email, platform: platformIntegrations.HUBSPOT }
      });
      if (!integrationRecord)
        return response.status(400).send({ msg: 'Issue with integration, no integration found' });
    
      const customerData = await this.customerRepository.findOne({
        where: { email }
      });
    
      let results = [];
      let hasMore = true;
      let vidOffset = null;
      // let params = 'property=firstname&property=lastname&property=address&property=city&property=state&property=zip&property=hs_state_code&property=betty_score';
      const existingColumnMapping:any = integrationRecord.column_mapping || {};

      const hubspotMappings = existingColumnMapping?.[platformIntegrations.HUBSPOT] || {};

      const paramsArray = Object.keys(hubspotMappings).map(column => `property=${column}`);
      paramsArray.push('property=betty_score');
      const params = paramsArray.join('&');
       let csvHeader=[];
      // Continue looping while there are more contacts to fetch
      while (hasMore) {

        // Include vi;dOffset in the API call if it exists
        let apiParams:any = {
          type: HubspotService.apiUrlsConstant.FetchContactsFromList.value,
          listId,
          params
        };
        if (vidOffset) {
          apiParams['vidOffset'] = vidOffset;  // Add vidOffset to the query params
        }
        
        let { status, data, msg } = await HubspotService.hubspotApiCall(
          this.integrationsRepository,
          integrationRecord,
          { type: HubspotService.apiUrlsConstant.FetchContactsFromList.value, listId, apiParams }
        );
    
        if (status === 500)
          return response.status(status).send({ msg });
    
        const { contacts, 'has-more': hasMoreFlag, 'vid-offset': nextVidOffset } = data;
        hasMore = hasMoreFlag;
        vidOffset = nextVidOffset;
    
        // if (!contacts || contacts.length === 0) break; // No more contacts to process
        csvHeader.push({ id: 'recordId', title: 'recordId' })
        
         for (const [hubspotColumn, hubspotProperty] of Object.entries(hubspotMappings) as [string,string][]) {
          csvHeader.push({ id: hubspotProperty, title: hubspotProperty  }); // Format header title
        }
        //If no contacts to process
        if(contacts.length===0){

          return response.status(200).send({data:[],msg:"No contacts to sync"});
        }
        for (const contact of contacts) {
          const contactResult: any = { recordId: contact.vid || '' };

           for (const [hubspotColumn, hubspotProperty] of Object.entries(hubspotMappings) as [string,string][]) {
             const value = contact.properties[hubspotColumn]?.value || '';  
            contactResult[hubspotProperty] = value;             
          }
          results.push(contactResult);  
          //  if (!contact.properties.betty_score?.value) {
          //   results.push(contactResult);  
          // }
          
        }
        if (!hasMore) break; // If there are no more contacts to fetch
      }
      const filename = `hubspot_${Date.now()}.csv`;
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
        source: platformIntegrations.HUBSPOT
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
        this.generateLeadsRepository
      );
      
      return response.status(200).send({
        results,
      });
    
    } catch (error) {
      console.log(error);
      if (error.response?.status === 401) {
        return response.status(401).send({ msg: 'Unauthorized: ' + error.message });
      }
      if (error.response?.status === 409 || error.response?.status === 429) {
        return response.status(error.response.status).send({ msg: 'Error: ' + error.message });
      }
      return response.status(500).send({ error: 'Error fetching contacts', details: error.message });
    }
    
  }


  @authenticate('jwt')
  @get(`${baseUrl}/total-lead-scored`, {
    responses: {
      '200': {
        description: 'Total contacts synced with hubspot',
        content: { 'application/json': { schema: { type: 'object' } } },
      },
    },
  })
  async getLeadsScoredData(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any>> {
    try {
      // TODO Integrate in integrations api
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');

      const records = await this.fileHistoryRepository.find({ where: { email } });
      const recordCount = records.filter((file: any) => file?.source == platformIntegrations.HUBSPOT).reduce((total, record) => {
        return total + (Number(record.record_count) || 0);
      }, 0);;
      return response.status(200).send({ recordCount }); // Return as an object for clarity

    } catch (error) {
      console.error('Error :', error.message);
      return response.status(500).send({ error: 'Error ', details: error.message });
    }
  }

  @authenticate('jwt')
  @get(`${baseUrl}/filtered-list`, {
    responses: {
      '200': {
        description: 'Get filtered list from HubSpot',
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
    // await new Promise((_, reject) => reject(new Error('Simulated async unhandled error')));

    const integrationsData = await this.integrationsRepository.findOne({
      where: { email, platform: platformIntegrations.HUBSPOT },
    });

    // Handle missing integration data
    if (!integrationsData) {
      return response.status(400).send({ msg: 'Issue with Integration' });
    }
    // Try to fetch the filtered lists from HubSpot
    let { status, data, msg } = await HubspotService.hubspotApiCall(
      this.integrationsRepository,
      integrationsData,
      { type: HubspotService.apiUrlsConstant.GetFilteredLists.value }
    );

    if (status == 200) {
      // Map the response to only return listId, name, and listType
      filteredLists = data.lists.map((list: any) => ({
        listId: list.listId,
        name: list.name,
        listType: list.listType,
      }));
    }
    return response.status(status).send({
      msg,
      data: filteredLists,
    });
  }

  @authenticate('jwt')
  @get(`${baseUrl}/disconnect`, {
    responses: {
      '200': {
        description: 'Disconnect hubspot',
        content: {
          'application/json': {
            schema: { type: 'object' },
          },
        },
      },
    },
  })
  async disconnectHubspot(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any>> {
    const responseFromFunction = await disconnectIntegration(this.integrationsRepository, request?.headers?.authorization || '', platformIntegrations.HUBSPOT);
    return response.status(responseFromFunction.status).send({ msg: responseFromFunction.msg, data: responseFromFunction.data });
  }

  @authenticate('jwt')
  @get(`${baseUrl}/properties`, {
    responses: {
      '200': {
        description: 'HubSpot Contact Property Names',
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
        where: { email, platform: platformIntegrations.HUBSPOT }
      });
    
      if (!integrationsData) {
        return response.status(400).send({ msg: 'No integration found for the given email' });
      }
    
      // Step 3: Make API call to fetch all contact properties from HubSpot
      const { status, data, msg } = await HubspotService.hubspotApiCall(
        this.integrationsRepository,
        integrationsData,
        { type: HubspotService.apiUrlsConstant.GetAllContactProperties.value }
      );
    
      // Step 4: Handle the response from the API call
      if (status !== 200) {
        return response.status(status).send({ msg: `Error fetching properties: ${msg}` });
      }
    
      // Step 5: Extract property names and send success response
      const propertyNames = data.results.map((property: any) => property.name);
      return response.status(200).send({ msg: 'Properties fetched successfully', data: propertyNames });
    
    } catch (error) {
      console.error('Error fetching properties from HubSpot:', error.message || error);
      return response.status(500).send({ msg: 'Failed to fetch properties from HubSpot', error: error.message });
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
    const responseFromFunction = await UpdateColumnMapping(this.integrationsRepository, request?.headers?.authorization || '', platformIntegrations.HUBSPOT, body.updated_column_mappings);
    return response.status(responseFromFunction.status).send({ msg: responseFromFunction.msg, data: responseFromFunction.data });
  }
}
