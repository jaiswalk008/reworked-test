import {
    get,
    post,
    requestBody,
    RestBindings,
    Response,
    Request,
    response,
  } from '@loopback/rest';
  import { inject } from "@loopback/core";
  import {
    CustomerRepository, IntegrationsRepository
  } from "../repositories";
  import { repository } from '@loopback/repository';
  import { platformIntegrations } from '../constant/platform_integrations';
  import { authenticate } from '@loopback/authentication';
  import { extractDetailsFromAuthToken } from '../helper/utils';
   
  const baseUrl = '/google-drive';

  
  export class GoogleDriveController {
    constructor(
      @repository(CustomerRepository)
      public customerRepository: CustomerRepository,
      @repository(IntegrationsRepository)
      public integrationsRepository: IntegrationsRepository,
    ) { }
    

  // update-destination-address
  @authenticate('jwt')
  @post(`${baseUrl}/update-destination-address`)
  @response(200, {
    description: 'Update update-destination-address',
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
  async updateDestinationAddress(
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'object',
            required: ['destination-address'],
            properties: {
              destination: {
                type: 'string',
              },
            },
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any, Record<string, any>>> {
    try{
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');

      const existingRecord = await this.integrationsRepository.findOne({
        where: { email, platform: platformIntegrations.GOOGLEDRIVE }
      });    

      const integrationData = {
        email,
        metadata: {
          destination_address: request.body?.destination_address
        },
        platform: platformIntegrations.GOOGLEDRIVE,
      };

      if (existingRecord) {
        await this.integrationsRepository.updateById(existingRecord.id, integrationData);
      } else {
        await this.integrationsRepository.create(integrationData);
      }

      return response.status(200).send({
        msg: "Destination address updated successfully",
        data: {}
      })
    }catch(error){
      console.log("Error in update-destination-address api of customer", error.message);
      return response.status(500).send({ msg: 'Error fetching update-destination-address', date: {details: error.message }});
    }
  }
}
  