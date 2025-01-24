import {Entity, model, property} from '@loopback/repository';

@model()
export class Promo extends Entity {
  @property({
    type: "string",
    id: true,
    generated: true,
  })
  id?: number;

  @property({
    type: "string",
    required: true,
  })
  offer_name: string;
  @property({
    type: "string",
    required: true,
  })
  coupon_code: string;
  @property({
    type: "string",
    required: true,
  })
  promotion_type: string;
  @property({
    type: "number",
    required: true,
  })
  discount_percent: number;
  @property({
    type: "string",
    required: true,
  })
  desc: string;
  @property({
    type: "date",
    required: true,
  })
  start_date: Date;
  @property({
    type: "date",
    required: true,
  })
  end_date: Date;
  @property({
    type: "array",
    itemType: "string",
    required: true,
  })
  applicable_pricing_plans: string[];
  constructor(data?: Partial<Promo>) {
    super(data);
  }
}

export interface PromoRelations {
  // describe navigational properties here
}

export type PromoWithRelations = Promo & PromoRelations;
