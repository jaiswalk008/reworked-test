import { inject } from "@loopback/core";
import { repository } from "@loopback/repository";
import {
  calculateRowsLeftForUser, getPriceFromRange, sendEmailToAdmin, getFirstAndLastDateOfMonth, creditsUsedForDates,
  calculateCreditUsageAccountWise,
  flattenDataForCSV
} from '../helper';
import {
  del,
  get,
  post,
  Request,
  requestBody,
  response,
  Response,
  RestBindings,
} from "@loopback/rest";
import { Referral } from "../models";
import {
  CustomerRepository, ReferralRepository, FileHistoryRepository
} from "../repositories";
import { ReferralsService } from "../services";
import { authenticate } from "@loopback/authentication";

export class ReferralController {
  constructor(
    @repository(CustomerRepository)
    public customerRepository: CustomerRepository,
    @repository(ReferralRepository)
    protected referralRepository: ReferralRepository,

    @repository(FileHistoryRepository)
    protected fileHistoryRepository: FileHistoryRepository,

  ) { }

  @authenticate('jwt')
  @post(`/referral`, {
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
  async upsertReferral(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["company_name", "company_poc_email", "referral_code"],
            properties: {
              referral_id: {
                type: "string",
              },
              company_name: {
                type: "string",
              },
              company_poc_name: {
                type: "string",
              },
              company_poc_email: {
                type: "string"
              },
              integration_key: {
                type: "string"
              },
              referral_code: {
                type: "string"
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
      const { company_poc_email, company_name, referral_id, company_poc_name, referral_code, integration_key } = requestBody;

      if (referral_id) {
        // Fetch existing referral data
        let existingReferral = await this.referralRepository.findOne({
          where: {
            id: referral_id,
          }
        });

        if (!existingReferral) {
          return response.status(404).send({ msg: 'Referral not found' });
        }

        // Update only the fields present in the request body
        existingReferral.company_name = company_name || existingReferral.company_name;
        existingReferral.company_poc_email = company_poc_email || existingReferral.company_poc_email;
        existingReferral.company_poc_name = company_poc_name || existingReferral.company_poc_name;
        existingReferral.referral_code = referral_code || existingReferral.referral_code;
        existingReferral.integration_key = integration_key || existingReferral.integration_key;

        // Save the updated referral
        await this.referralRepository.update(existingReferral, {
          where: {
            id: referral_id
          }
        });

        const updatedReferralData = await this.referralRepository.findOne({
          where: {
            id: referral_id,
          }
        });

        return response.status(200).send({ msg: 'Referral updated successfully', data: updatedReferralData });
      } else {

        const [updatedReferralData, existingReferralCode] = await Promise.all([
          this.referralRepository.findOne({ where: { company_poc_email: company_poc_email } }),
          this.referralRepository.findOne({ where: { referral_code: referral_code } })
        ]);
        if (updatedReferralData)
          return response.status(404).send({ msg: 'Email id already exists' });
        if(existingReferralCode){
          return response.status(404).send({ msg: 'Referral code already exists' });
        }
        // Create a new referral
        const referralUpdateObj = new Referral();
        referralUpdateObj.company_name = company_name;
        referralUpdateObj.company_poc_email = company_poc_email;
        referralUpdateObj.company_poc_name = company_poc_name;
        referralUpdateObj.referral_code = referral_code;
        referralUpdateObj.integration_key = integration_key;
        const newReferral = await this.referralRepository.create(referralUpdateObj);

        return response.status(200).send({ msg: 'Referral created successfully', data: newReferral });
      }
    } catch (error) {
      console.error('Error in referrals controller /referral', error);
      return response.status(500).send({ msg: error.message });
    }

  }


  @authenticate('jwt')
  @get(`/referral`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Get Referrals',
      },
    },
  })
  async getReferrals(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: [],
            properties: {
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
      const referralData = await this.referralRepository.find();
      return response.status(200).send({ msg: 'Referral fetched successfully', data: referralData });
    } catch (error) {
      console.error('Error in referrals controller get referral', error);
      return response.status(500).send({ msg: error.message });
    }

  }

  @authenticate('jwt')
  @post(`/referral/reports`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Get Referrals',
      },
    },
  })
  async getReports(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: ["referral_id"],
            properties: {
              history: {
                type: "boolean",
              },
              referral_id: {
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
      const { history, referral_id } = requestBody;
      const referralData = await this.referralRepository.findOne({
        where: {
          id: referral_id
        }
      });

      if (!referralData)
        return response.status(404).send({ msg: 'Invalid referral id' });

      const customerData = await this.customerRepository.find({
        where: {
          referrer: referralData.referral_code,
        }
      });

      if (customerData?.length) {
        const responseData = await ReferralsService.fetchReports(this.fileHistoryRepository, customerData, referralData)
        return response.status(responseData.status).send({ ...responseData });
      }

      return response.status(200).send({ msg: 'No users found with this referral', data: {} });
    } catch (error) {
      console.error('Error in referrals controller get referral', error);
      return response.status(500).send({ msg: error.message });
    }
  }

  

  @authenticate('jwt')
  @del('/referral', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Delete referral',
      },
    },
  })
  async deleteReferral(
    @requestBody({
      content: {
        'application/json': {
          schema: {
            type: 'object',
            required: ['referral_id'],
            properties: {
              referral_id: {
                type: 'string',
              },
            },
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<any>> {
    try {
      await this.referralRepository.deleteById(request.body.referral_id);
      return response.status(200).send({ msg: 'Referral deleted successfully' });
    } catch (error) {
      console.error('Error in referrals controller delete referral', error);
      return response.status(500).send({ msg: error.message });
    }
  }
  
}



