import { Entity, model, property } from '@loopback/repository';

@model()
export class Referral extends Entity {
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
  company_name: string;

  @property({
    type: 'string',
    required: true,
  })
  company_poc_name: string;

  @property({
    type: 'string',
    required: true,
    index: {
      unique: true,
    },
  })
  company_poc_email: string;

  @property({
    type: 'string',
    required: false,
  })
  referral_code: string;

  @property({
    type: 'string',
    required: false,
  })
  integration_key: string;

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
  constructor(data?: Partial<Referral>) {
    super(data);
  }
}

export interface ReferralRelations {
  // describe navigational properties here
}

export type ReferralRelationWithRelations = Referral & ReferralRelations;
