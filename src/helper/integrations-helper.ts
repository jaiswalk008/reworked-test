import { extractDetailsFromAuthToken } from "./utils";
import { IntegrationsRepository } from "../repositories";
import axios from "axios";

export const disconnectIntegration = async (integrationsRepository: IntegrationsRepository, authorization: string, platformType: string) => {
    try {
        const { email } = extractDetailsFromAuthToken(authorization || '');
        const existingRecord = await integrationsRepository.findOne({
            where: { email, platform: platformType }
        });
        if (existingRecord)
            await integrationsRepository.deleteById(existingRecord.id);

        return { msg: `${platformType} disconnected Successfully`, data: null, status: 200 };
    } catch (error) {
        const msg  = `Error in ${platformType} disconnection:`;
        console.error(`${msg}, ${error.message}`);
        return { msg, data: { details: error.message }, status: 500 };
    }
}


export const UpdateColumnMapping = async (integrationsRepository: IntegrationsRepository, authorization: string, platformType: string, columnMapping: any) => {
    try {
        const { email } = extractDetailsFromAuthToken(authorization || '');

        const integrationsData = await integrationsRepository.findOne({
            where: { email, platform: platformType },
        });

        if (!integrationsData) {
            return { msg: `${platformType} Issue with ${platformType} Integrations, No integration found`, data: null, status: 500 };
        }
        const existingColumnMapping: any = integrationsData.column_mapping || {};

        const updatedData = {
            ...integrationsData,
            column_mapping: {
                ...existingColumnMapping,
                [platformType]: {
                    ...existingColumnMapping?.[platformType],
                    ...columnMapping,
                }
            },
            updated_at: new Date().toISOString(),
        };
        await integrationsRepository.updateById(integrationsData.id, updatedData);
        return { msg: `Column mappings updated successfully`, data: null, status: 200 };
    } catch (error) {
        const msg  = `Error in ${platformType} UpdateColumnMapping:`;
        console.error(`${msg}, ${error.message}`);
        return { msg, data: { details: error.message }, status: 500 };
    }
}

export const getClientRegion = async (): Promise<{ region: string }> => {
    try {
      const ipResponse = await axios.get('https://api.ipify.org?format=json');
      const ip: string = ipResponse.data.ip;  // Public IP of the client
  
      const geoResponse = await axios.get(`https://ipapi.co/${ip}/json/`);
      const countryCode: string = geoResponse.data.country; 
      let region: string;
  
      switch (countryCode) {
        case 'IN':
            region = 'in'  
          break;
        case 'US':
            region = 'us';  
          break;
        case 'EU':
        case 'GB':
        case 'FR':  
        region = 'eu';  
          break;
        default:
            region = 'us';  
          break;
      }
      
      return { region };
    } catch (error) {
      console.error('Error fetching IP or geolocation:', error.message);
      return {  region: 'us' };  
    }

  };
  
