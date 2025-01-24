import { inject } from '@loopback/core';
import {
  get,
  param,
  Response,
  Request,
  requestBody,
  post,
  response,
  RestBindings,
} from '@loopback/rest';
import fs from 'fs';
import { promisify } from 'util';
import { STORAGE_DIRECTORY } from '../keys';
import { TokenService, authenticate } from '@loopback/authentication';
import { SurveyRepository } from '../repositories';
import { TokenServiceBindings, UserRepository } from '@loopback/authentication-jwt';
import {
  repository,
} from '@loopback/repository';
import { sendTwit } from '../helper/integrations'

/**
 * A controller to handle file downloads using multipart/form-data media type
 */

export class surveyController {
  constructor(@inject(TokenServiceBindings.TOKEN_SERVICE)
    public jwtService: TokenService,
    @inject(STORAGE_DIRECTORY) private storageDirectory: string,
    @repository(SurveyRepository)
    protected surveyRepository: SurveyRepository) { }


  // @authenticate('jwt')
  // @get(`/survey`, {
  //   responses: {
  //     200: {
  //       content: {
  //         'application/json': {
  //           schema: {
  //             type: 'object',
  //           },
  //         },
  //       },
  //       description: 'Get Surey',
  //     },
  //   },
  // })
  // async getSurveys(
  //   @requestBody({
  //     content: {
  //       "application/json": {
  //         schema: {
  //           type: "object",
  //           required: [],
  //           properties: {
  //             email: {
  //               type: "string",
  //             },
  //           },
  //         },
  //       },
  //     },
  //   })
  //   @inject(RestBindings.Http.REQUEST)
  //   request: Request,
  //   @inject(RestBindings.Http.RESPONSE) response: Response,
  // ): Promise<object> {
  //   try {
  //     const surveys = await this.surveyRepository.find();
  //     return response.status(200).send({ msg: 'Survey Fetched Successfully', data: surveys });
  //   } catch (error) {
  //     console.log(error);
  //     return response.status(500).send({ msg: error.message });
  //   }
  // }


  // @authenticate('jwt')
  @post(`/survey`, {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Create Survey',
      },
    },
  })
  async createSurvey(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
                required: ["questionnaire", "name", "companyName", "email", "phone"],
            properties: {
                  question_answer : {
                type: "array",
              },
                  name : {
                type: "string",
              },
                  companyName : {
                type: "string",
              },
                  email : {
                type: "string",
              },
                  phone : {
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
      // const requestHeaders = request?.headers;
      const email = requestBody?.email;
      const question_answers = requestBody?.question_answer;
      let name = requestBody?.name;
      let companyName = requestBody?.companyName;
      let phone = requestBody?.phone;
      const surveyData = {
        email,
        name,
        companyName,
        phone,
        question_answers
      }

      const res = await this.surveyRepository.create(surveyData)
      return response.status(200).send({ msg: 'Survey Created Successfully', data: res });

    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }
  @authenticate('jwt')
  @post(`/tweet`, {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
            },
          },
        },
        description: "Create Survey",
      },
    },
  })
  async createTweet(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
            required: [],
            properties: {},
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<Response<any>> {
    try {
      const savingAmount = 10;
      await sendTwit(savingAmount)

      return response.status(200).send({ msg: "Tweet Successfully" });
    } catch (error) {
      console.log(error);
      return response.status(500).send({ msg: error.message });
    }
  }
}
