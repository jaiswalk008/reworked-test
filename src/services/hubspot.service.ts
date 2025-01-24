import { parseFileToJSON } from "../helper"
import axios from 'axios';
import { IntegrationsRepository } from '../repositories';
import { platformIntegrations } from "../constant/platform_integrations";
import { Integrations } from "../models";
/**
 * A provider to return an `Express` request handler from `multer` middleware
 */
export class HubspotService {

  static apiUrlsConstant: any = {
    FetchContactsFromList: {
      value: "FetchContactsFromList",
      api: `${process.env.HUBSPOT_API_BASE_URL}/contacts/v1/lists/$listId/contacts/all?$params`
    },
    GetFilteredLists: {
      value: "GetFilteredLists",
      api: `${process.env.HUBSPOT_API_BASE_URL}/contacts/v1/lists`
    },
    RefreshAccessToken: {
      value: "RefreshAccessToken",
      api: `${process.env.HUBSPOT_API_BASE_URL}/oauth/v1/token`
    },
    CreateProperty: {
      value: "CreateProperty",
      api: `${process.env.HUBSPOT_API_BASE_URL}/crm/v3/properties/contacts/batch/create`
    },
    UpdateHubspotRecord: {
      value: "UpdateHubspotRecord",
      api: `${process.env.HUBSPOT_API_BASE_URL}/crm/v3/objects/contacts/batch/update`
    },
    GetAllContactProperties: {
      value: "GetAllContactProperties",
      api: `${process.env.HUBSPOT_API_BASE_URL}/crm/v3/properties/contacts`
    },
  }
  static hubspotApiCall = async (
    integrationsRepository: IntegrationsRepository,
    integrationData: Integrations,
    options: any
  ) => {
    const { type, listId, params, method = 'GET', payload = null } = options; // Add payload option
    const accessToken = integrationData.access_token;
    let apiUrl = this.apiUrlsConstant[type].api; // Get the API URL based on type
    let requestOptions: any = {};
    try {
      if (listId) {
        apiUrl = apiUrl.replace("$listId", listId);
      }
      if (params) {
        apiUrl = apiUrl.replace("$params", new URLSearchParams(params).toString());
      }
  
      // Prepare the request options based on the method and payload
      requestOptions = {
        method,
        url: apiUrl,
        headers: {
          Authorization: `Bearer ${accessToken}`,
          'Content-Type': 'application/json',
        },
      };
  
      // Add the payload to the request body for POST, PUT, PATCH, etc.
      if (method === 'POST' || method === 'PUT' || method === 'PATCH') {
        requestOptions.data = payload; // Add the payload to the request
      }
      const response = await axios(requestOptions);
      return { status: 200, msg: "API Called Successfully", data: response.data };
  
    } catch (error) {
      // Handle expired access token (401 error)
      if (error.response?.status === 401) {
        const refreshToken = integrationData.refresh_token;
  
        try {
          // Refresh the access token using the refresh token
          const tokenResponse = await axios.post(`${process.env.HUBSPOT_API_BASE_URL}/oauth/v1/token`, null, {
            params: {
              grant_type: 'refresh_token',
              client_id: process.env.HUBSPOT_CLIENT_ID,
              client_secret: process.env.HUBSPOT_SECRET,
              refresh_token: refreshToken,
              redirect_uri: process.env.HUBSPOT_REDIRECT_URI,
            },
            headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          });
  
          // Update the access token in the database
          integrationData.access_token = tokenResponse.data.access_token;
          integrationData.updated_at = new Date();
          await integrationsRepository.update(integrationData);
  
          // Retry the API request with the new access token
          requestOptions.headers.Authorization = `Bearer ${integrationData.access_token}`;
          const retryResponse = await axios(requestOptions);
          return { status: retryResponse.status, msg: "API Called Successfully", data: retryResponse.data };
  
        } catch (refreshError) {
          return {
            status: refreshError.response?.status || 500,
            msg: 'Error refreshing token',
            data: { details: refreshError.response?.data || refreshError.message },
          };
        }
      } else {
        // Handle other errors
        return {
          status: error.response?.status || 500,
          msg: 'Error making API call',
          data: { details: error.response?.data || error.message },
        };
      }
    }
  };

  static updateHubspotContacts = async (
    integrationsRepository: IntegrationsRepository,
    fileName: string,
    email: string
  ) => {
    try {

      const integrationsData = await integrationsRepository.findOne({
        where: {
          email,
          platform: platformIntegrations.HUBSPOT
        }
      })
      if (integrationsData) {
        // Parse the CSV file and get the data
        const res = await parseFileToJSON(fileName, email);

        if (!res?.data?.parsedData) {
          throw new Error('No parsed data available from CSV');
        }

        const parsedData = res.data.parsedData;

        // The first row contains headers, so we'll skip it
        const headers = parsedData[0];
        const rows = parsedData.slice(1);
        console.log(rows)
        const batchContacts = rows.map((row: string[]) => {
          const recordId = row[headers.indexOf('recordId')];
          const bettyScore = row[headers.indexOf('BETTY SCORE')];
          const bettyPredicted = row[headers.indexOf('BETTY PREDICTED')];
          console.log(recordId, bettyScore, bettyPredicted)
          // Return the contact payload
          return {
            id: recordId,  // This will be the recordId (or any unique identifier)
            properties: {
              betty_score: bettyScore
            }

          };
        });

        // Split into smaller batches to avoid API limits
        const BATCH_SIZE = 100; // HubSpot recommends batches of 100 or fewer contacts
        for (let i = 0; i < batchContacts.length; i += BATCH_SIZE) {
          const batch = batchContacts.slice(i, i + BATCH_SIZE);
          await this.sendBatchUpdateToHubspot(integrationsRepository, integrationsData, batch);
        }

        console.log('All contacts updated successfully');

      }
    } catch (error) {
      console.error('Error updating HubSpot contacts:', error.message);
    }
  };

  static sendBatchUpdateToHubspot = async (integrationsRepository: IntegrationsRepository, integrationsData: Integrations, batch: any[]) => {
    try {
      // Step 1: Create property payload
      const propertyPayload = {
        inputs: [
          {
            hidden: false,
            displayOrder: 1,
            description: "Betty Score for contacts",
            label: "Betty Score",
            type: "number",
            formField: false,
            groupName: "contactinformation",
            name: "betty_score",
            fieldType: "number"
          }
        ]
      };
  
      // Create new property via HubSpot API (POST request)
      let propertyResponse = await HubspotService.hubspotApiCall(
        integrationsRepository,
        integrationsData,
        {
          type: HubspotService.apiUrlsConstant.CreateProperty.value,
          method: 'POST',
          payload: propertyPayload
        }
      );
  
      // Check if property creation was successful
      if (propertyResponse.status !== 200) {
        console.error('Error creating property:', propertyResponse.msg);
        return;
      }
  
      // Step 2: Prepare batch update payload
      const updatePayload = {
        inputs: batch.map(contact => ({
          id: contact.id, // Contact ID
          properties: contact.properties // Updated properties for the contact
        }))
      };
  
      // Send batch update via HubSpot API (POST request)
      let updateResponse = await HubspotService.hubspotApiCall(
        integrationsRepository,
        integrationsData,
        {
          type: HubspotService.apiUrlsConstant.UpdateHubspotRecord.value,
          method: 'POST',
          payload: updatePayload
        }
      );
  
      // Check if batch update was successful
      if (updateResponse.status === 200) {
        console.log('Batch update successful:', updateResponse.data);
      } else {
        console.error('Error in batch update:', updateResponse.msg);
      }
  
    } catch (error) {
      // Enhanced error handling for clarity
      console.error('Error in batch update:', error.response?.data || error.message);
    }
  };
  

}

