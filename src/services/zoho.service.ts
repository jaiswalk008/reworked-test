import axios from 'axios';
import { IntegrationsRepository } from '../repositories';
 import { platformIntegrations } from "../constant/platform_integrations";
import { Integrations } from "../models";
import { parseFileToJSON } from "../helper"
import { getClientRegion } from '../helper/integrations-helper';

export class ZohoService {
    static region ='';
    static apiUrlsConstant = {
        FetchContacts: {
          value: "FetchContacts",
          api: `/crm/v7/Contacts?fields=$params`,
        },
        RefreshAccessToken: {
          value: "RefreshAccessToken",
          api: `/oauth/v2/token`,
        },
        CreateProperty: {
          value: "CreateProperty",
          api: `/crm/v7/settings/fields?module=Contacts`, // Endpoint to create a new property
        },
        UpdateZohoRecord: {
          value: "UpdateZohoRecord",
          api: `/crm/v7/Contacts/upsert`, // Endpoint to update contacts
        },
        GetAllContactProperties: {
          value: "GetAllContactProperties",
          api: `/crm/v7/settings/fields?module=Contacts`
        },
        GetFilteredLists: {
          value: "GetFilteredLists",
          api: `/crm/v7/settings/custom_views?module=Contacts`
        },
    }

  static zohoApiCall = async (
    integrationsRepository: IntegrationsRepository,
    integrationData: Integrations,
    options: any
  ) => {
     const { type, params, method = 'GET', payload = null } = options as { 
        type: 'FetchContacts' | 'RefreshAccessToken'; 
        params: any; 
        totalLeads: number; 
        method?: string; 
        payload?: any; 
      };
      //for testing
      this.region = 'in';
       let zohoApiURL
      if(this.region!=='us'){
        const zohoAuthURLArray = process.env.ZOHO_API_BASE_URL?.split('.') as string[]
        zohoAuthURLArray[2]=this.region;
        zohoApiURL = zohoAuthURLArray.join('.')
      }
      else{
        zohoApiURL = process.env.ZOHO_API_BASE_URL
      }
    const accessToken = integrationData.access_token;
    let apiUrl =`${zohoApiURL}${this.apiUrlsConstant[type].api}`;
    let requestOptions: any = {};
    try {

      if(params){
            apiUrl = apiUrl.replace("$params", params)
        }

        requestOptions = {
        method,
        url: apiUrl,
        headers: {
          Authorization: `Zoho-oauthtoken ${accessToken}`,
          'Content-Type': 'application/json',
        },
      };

      if (['POST', 'PUT', 'PATCH'].includes(method)) {
        requestOptions.data = payload;
      }
   
      const response = await axios(requestOptions);
      return { status: response.status, msg: "API Called Successfully", data: response.data };
    } catch (error) {
      if (error.response?.status === 401) {
        const refreshToken = integrationData.refresh_token;
        const tokenUrl = `${process.env.ZOHO_AUTH_URL}${this.region}/oauth/v2/token?refresh_token=${refreshToken}&client_id=${process.env.ZOHO_CLIENT_ID}&client_secret=${process.env.ZOHO_CLIENT_SECRET}&grant_type=refresh_token`;
        try {
            const tokenResponse = await axios.post(tokenUrl, null, {
                headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
              });

          integrationData.access_token = tokenResponse.data.access_token;
          integrationData.updated_at = new Date();
          await integrationsRepository.update(integrationData);

          requestOptions.headers.Authorization = `Zoho-oauthtoken ${integrationData.access_token}`;
          const retryResponse = await axios(requestOptions);
          return { status: retryResponse.status, msg: "API Called Successfully", data: retryResponse.data};
        } catch (refreshError) {
          return {
            status: refreshError.response?.status || 500,
            msg: 'Error refreshing token',
            data: { details: refreshError.response?.data || refreshError.message },
          };
        }
      } else {
        console.log(error)
        if(error.response?.status===400 && error.response.data.fields[0].code==='DUPLICATE_DATA'){
            return {
                status: error.response?.status || 400,
                msg: error.response.data.fields[0].code,
                data: error.response.data.fields
              };
        }
        return {
          status: error.response?.status || 500,
          msg: 'Error making API call',
          data: { details: error.response?.data || error.message  },
        };
      }
    }
  };
  static updateZohoContacts = async (
    integrationsRepository: IntegrationsRepository,
    fileName: string,
    email: string
  ) => {
    try {
      const integrationsData = await integrationsRepository.findOne({
        where: {
          email,
          platform: platformIntegrations.ZOHO  
        }
      });
  
      if (integrationsData) {
        // Parse the CSV file and get the data
        const res = await parseFileToJSON(fileName, email);
  
        if (!res?.data?.parsedData) {
          throw new Error('No parsed data available from CSV');
        }
  
        const parsedData = res.data.parsedData;
  
         const headers = parsedData[0];
        const rows = parsedData.slice(1);
  
        const batchContacts = rows.map((row: string[]) => {
          const contactId = row[headers.indexOf('contactId')];
          const bettyScore = row[headers.indexOf('BETTY SCORE')];
  
          // Return the contact payload
          return {
            id: contactId,  // This will be the contactId (or any unique identifier)
            properties: {
              Betty_Score: bettyScore
            }
          };
        });
  
        // Split into smaller batches to avoid API limits
        const BATCH_SIZE = 100; // Zoho also recommends batches of 100 or fewer contacts
        for (let i = 0; i < batchContacts.length; i += BATCH_SIZE) {
          const batch = batchContacts.slice(i, i + BATCH_SIZE);
          await this.sendBatchUpdateToZoho(integrationsRepository, integrationsData, batch);
        }
  
        console.log('All contacts updated successfully');
      }
    } catch (error) {
      console.error('Error updating Zoho contacts:', error.message);
    }
  };
  
 static sendBatchUpdateToZoho = async (integrationsRepository: IntegrationsRepository, integrationsData: Integrations, batch: any[]) => {
    try {
      // Step 1: Create property payload
      const propertyPayload = {
        fields: [
          {
            field_label: "Betty Score",
            data_type: "text", 
            length: 20, 
            filterable: true,
            tooltip: {
              name: "static_text",
              value: "Enter Betty Score"
            },
            crypt: {
              mode: "decryption" 
            }
          }
        ]
      };
      
  
      // Create new property via Zoho API (POST request)
      let propertyResponse = await ZohoService.zohoApiCall(
        integrationsRepository,
        integrationsData,
        {
          type: ZohoService.apiUrlsConstant.CreateProperty.value,
          method: 'POST',
          payload: propertyPayload
        }
      );
 
        if (propertyResponse.status !== 200) {
            const { status, msg } = propertyResponse;

            if (status === 400 && msg === 'DUPLICATE_DATA') {
              console.log('Field label duplicate, continuing execution...');
            } else {
              console.error('Error creating property in Zoho:', msg);
              return;
            }
          }
      const updatePayload = {
        data: batch.map(contact => ({
          id: contact.id, 
          ...contact.properties 
        })),
        duplicate_check_fields: ["id"]  
      };
      let updateResponse = await ZohoService.zohoApiCall(
        integrationsRepository,
        integrationsData,
        {
          type: ZohoService.apiUrlsConstant.UpdateZohoRecord.value,
          method: 'POST',
          payload: updatePayload
        }
      );
  
      if (updateResponse.status === 200) {
        console.log('Batch update successful in Zoho:', updateResponse.data);
      } else {
        console.error('Error in batch update to Zoho:', updateResponse.msg);
      }
    } catch (error) {
      console.error('Error in batch update to Zoho:', error.response?.data || error.message);
    }
  }; 
  static async initializeRegion() {
    if(!this.region){
      const {region} = await getClientRegion();
      this.region = region
    }
    
  }

  
}
