import { Entity, model, property } from '@loopback/repository';
import { industryTypes } from '../constant/industry_type';

@model()
export class Customer extends Entity {
  @property({
    type: 'string',
    id: true,
    generated: true,
  })
  id?: string;

  @property({
    type: 'string',
    required: true,
  })
  name: string;

  @property({
    type: 'string',
    required: true,
  })
  email: string;

  @property({
    type: 'string',
    required: false,
  })
  referrer: string;

  @property({
    type: 'any',
    required: false,
  })
  stripe_customer_id: any;

  // @property({
  //   type: 'boolean',
  //   required: false,
  // })
  // isAdmin: boolean;

  @property({
    type: 'any',
    required: false,
  })
  row_credits: number;

  @property({
    type: 'any',
    required: false,
  })
  lead_gen_row_credits: number;

  @property({
    type: 'any',
    required: false,
    default: 0
  })
  roll_over_credits: number

  @property({
    type: 'array',
    itemType: 'object',
    required: true,
  })
  login_history: object[];

  @property({
    type: 'number',
    required: false
  })
  custom_rows_per_month?: number;

  @property({
    type: 'array',
    itemType: 'object',
    required: false,
  })
  file_history: object[];

  @property({
    type: 'object',
    itemType: 'any',
    required: false,
  })
  pricing_plan: {
    stripe_subscription_status: string,
    subscription_id: string,
    current_period_end: number,
    plan: string | null | undefined,
    stripe_price_id: string,
    stripe_product_id: string,
    stripe_payment_method_id: string,
    stripe_invoice_id: string,
    stripe_payment_intent_status: string,
    stripe_payment_intent_client_secret: string,
    resume_paused_subscription_at: Date | null | undefined,
    start_paused_subscription_at: Date | null | undefined,
    auto_debit: Boolean | null
  }

  @property({
    type: 'array',
    itemType: 'object',
    required: false,
  })
  subscription_log: object[]

//   @property({
//     type: 'array',
//     itemType: 'object',
//     required: false,
//   })
//   investment_profile: InvestmentProfile[]


  @property({
    type: 'array',
    itemType: 'object',
    required: false,
  })
  per_unit_price: object[]

  @property({
    type: 'boolean',
    default: false
  })
  isCouponRedeemed: Boolean

//   @property({
//     type: 'string',
//     itemType: 'string',
//     // enum: industryTypes.AVAILABLE_INDUSTRIES,
//     required: false,
//   })
//   industry_type: string

  @property({
    type: 'string',
    required: false,
    enum: ["user", "admin", "support"],
    default: "user"
  })
  role: string;

  @property({
    type: 'object',
    required: false,
  })
  lead_sorting_default_model: object;

  @property({
    type: 'string',
    required: false,
    default: null
  })
  api_secret_key: string;

  // @property({
  //   type: 'array',
  //   itemType: 'object',
  //   required: false,
  // })
  // sub_accounts: [{
  //   email: string,
  //   name: string,
  // }]


  @property({
    type: 'string',
    required: false,
    default: null
  })
  source: string;


  @property({
    type: 'array',
    itemType: 'object',
    required: false,
  })
  lead_gen_per_unit_price: object[]

  @property({
    type: 'string',
    required: false,
  })
  parent_email: string;
  
  @property({
    type: 'object',
    required: true,
    default: {
      external_order_id: false,
      disable_post_process_validation: false,
      custom_model: null,
      file_id_generate:false,
    },
  })
  add_ons: any

  @property({
    type:'object',
    required:true,
    default:{
      answered:false,
      platform:'',
    }
  })
  survey_answer:object;

  constructor(data?: Partial<Customer>) {
    super(data);
  }
}

export interface CustomerRelations {
  // describe navigational properties here
}

export type CustomerRelationWithRelations = Customer & CustomerRelations;
