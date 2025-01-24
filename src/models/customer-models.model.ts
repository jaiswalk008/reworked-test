import { Entity, model, property } from "@loopback/repository";
// import { InvestmentProfile } from '../types/investment_profile';
import { IndustryProfile } from '../types/industry_profile';
import { ModelCriteria } from '../types/model_criteria';
import { ModelInsights } from '../types/model_insights';
import { industryTypes } from '../constant/industry_type';

@model()
export class CustomerModels extends Entity {
  @property({
    type: "string",
    id: true,
    generated: true,
  })
  id?: string;
  @property({
    type: "string",
    required: true,
  })
  email: string;
  @property({
    type: "string",
    required: true,
  })
  name: string;
  @property({
    type: "string",
    required: false,
  })
  description: string;
  @property({
    type: "object",
    required: false,
  })
  criteria: ModelCriteria;
  @property({
    type: "object",
  })
  zipcode_sorted_list: object[];

  @property({
    type: "string",
  })
  vendor_list_url: string;

  @property({
    type: "string",
  })
  file_extension: string;
  @property({
    type: "string",
  })
  error_detail: string;
  
  @property({
    type: "string",
  })
  error: string;


  @property({
    type: "string",
  })
  type: string;

  @property({
    type: "number",
  })
  status: number;

  @property({
    type: "number",
    default: 0
  })
  row_count: number;
  
  @property({
    type: "date",
    required: true,
    default: () => new Date(),
  })
  created_at: Date;
  @property({
    type: "date",
    required: true,
    default: () => new Date(),
  })
  updated_at: Date;
  @property({
    type: 'object',
    required: false,
  })
  industry_profile: IndustryProfile
//   @property({
//     type: 'object',
//     required: false,
//   })
//   investment_profile: InvestmentProfile
  @property({
    type: 'object',
    required: false,
  })
  insights: ModelInsights

//   @property({
//     type: 'string',
//     itemType: 'string',
//     enum: industryTypes.AVAILABLE_INDUSTRIES,
//     required: false,
//   })
//   industry_type: string

  @property({
    type: 'boolean',
    required: false,
  })
  default: boolean;

  @property({
    type: 'number',
    required: false,
  })
  payment_status: number;

  @property({
    type: "string",
  })
  industry_profile_id: string;

  constructor(data?: Partial<CustomerModels>) {
    super(data);
  }
  
}

export interface CustomerModelsRelations {
  // describe navigational properties here
}

export type CustomerModelsWithRelations = CustomerModels & CustomerModelsRelations;
