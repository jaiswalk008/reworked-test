import {
    repository
  } from '@loopback/repository';
  import {
    post,
    requestBody,
    response,RestBindings,Response
  } from '@loopback/rest';
import { inject } from "@loopback/core";

import { RoiCalculator } from '../models/roicalculator.model';
import { RoiCalculatorRepository } from '../repositories/roicalculator.repository';
import { sendEmailToAdmin } from '../helper';
  
  const IMPROVEMENT_FACTORS = {
    contactRate: 1.35,  
    appointmentRate: 1.25,  
    closeRate: 1.20  
  };
  
  export class RoiCalculatorController {
    constructor(
      @repository(RoiCalculatorRepository)
      public roiCalculatorRepository: RoiCalculatorRepository,
    ) {}
  
    private calculateCurrentMetrics(inputs: any) {
      const contacts = Math.floor((inputs.monthlyLeads * inputs.contactRate) / 100);
      const appointments = Math.floor((contacts * inputs.appointmentRate) / 100);
      const deals = Math.floor((appointments * inputs.closeRate) / 100);
      const revenue = Math.floor(deals * inputs.avgJobValue);
  
      return { contacts, appointments, deals, revenue };
    }
  
    private calculateImprovedMetrics(inputs: any) {
      const contacts = Math.floor((inputs.monthlyLeads * inputs.contactRate * IMPROVEMENT_FACTORS.contactRate) / 100);
      const appointments = Math.floor((contacts * inputs.appointmentRate * IMPROVEMENT_FACTORS.appointmentRate) / 100);
      const deals = Math.floor((appointments * inputs.closeRate * IMPROVEMENT_FACTORS.closeRate) / 100);
      const revenue = Math.floor(deals * inputs.avgJobValue);
  
      return { contacts, appointments, deals, revenue };
    }
  
    @post('/roi-calculation', {
      responses: {
        '200': {
          description: 'Calculate ROI',
          content: {
            'application/json': {
              schema: { type: 'object' },
            },
          },
        },
      },
    })
    async calculateRoi(
      @requestBody({
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                monthlyLeads: { type: 'string' },
                contactRate: { type: 'string' },
                appointmentRate: { type: 'string' },
                closeRate: { type: 'string' },
                avgJobValue: { type: 'string' },
                email: { type: 'string' },
              },
              required: ['monthlyLeads', 'contactRate', 'appointmentRate', 'closeRate', 'avgJobValue', 'email'],
            },
          },
        },
      }) inputs: any,
      @inject(RestBindings.Http.RESPONSE) response: Response,
    ): Promise<Response<any>> {
      try {
         const record = await this.roiCalculatorRepository.create({
          email: inputs.email,
          input_params: inputs,
          calculation_results: {},
          created_at: new Date(),
        });
     //   const current = this.calculateCurrentMetrics(inputs);
    //   const improved = this.calculateImprovedMetrics(inputs);
  
    //   const additionalDeals = improved.deals - current.deals;
    //   const additionalRevenue = improved.revenue - current.revenue;
    //   const percentageImprovement = ((improved.revenue - current.revenue) / current.revenue) * 100;
  
    //   const results = {
    //     current,
    //     improved,
    //     improvements: {
    //       additionalDeals,
    //       additionalRevenue,
    //       percentageImprovement,
    //     },
    //   };
  
         const optionsforAdminMail = {
          content: `Customer with email ${inputs.email} has submitted the ROI calculation form. 
          Please check the input values:
          - Monthly Leads: ${inputs.monthlyLeads}
          - Contact Rate: ${inputs.contactRate}
          - Appointment Rate: ${inputs.appointmentRate}
          - Close Rate: ${inputs.closeRate}
          - Avg Job Value: ${inputs.avgJobValue}`,
        };
        sendEmailToAdmin('', { name: '', email: inputs.email }, null, optionsforAdminMail);
    
        return response.status(200).send({
          success: true,
          message: 'ROI calculation submitted successfully.',
        });
      } catch (error) {
        console.error('Error processing ROI calculation:', error.message);
        return response.status(500).send({
          success: false,
          message: 'Error processing ROI calculation.',
          error: error.message,
        });
      }
    }  
  }