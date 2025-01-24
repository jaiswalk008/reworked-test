import {Entity, model, property} from '@loopback/repository';
import { IndustryProfile } from '../types/industry_profile';
import { industryTypes } from '../constant/industry_type';

@model()
export class CustomerIndustry extends Entity {
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
  email: string;

  @property({
    type: 'string',
    required: true,
    enum: industryTypes.AVAILABLE_INDUSTRIES,
  })
  industry_type: string;

  @property({
    type: 'array',
    itemType: 'object',
    required: false
  })
  industry_profile: IndustryProfile[];

  // Indexer property to allow additional data
  // eslint-disable-next-line @typescript-eslint/no-explicit-any
  [prop: string]: any;

  constructor(data?: Partial<CustomerIndustry>) {
    super(data);
  }
}

export interface CustomerIndustryRelations {
  // describe navigational properties here
}

export type CustomerIndustryWithRelations = CustomerIndustry & CustomerIndustryRelations;
