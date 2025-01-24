import {Entity, model, property} from '@loopback/repository';

@model()
export class RoiCalculator extends Entity {
  @property({
    type: 'string',
    id: true,
    generated: true,
  })
  id?: number;

  @property({
    type: 'string',
    required: true,
  })
  email: string;

  @property({
    type: 'object',
    required: true,
  })
  input_params: object;

 @property({
    type: 'object',
    required: true,
  })
  calculation_results: object;

  @property({
    type: 'date',
    required: true,
  })
  created_at: Date;
  constructor(data?: Partial<RoiCalculator>) {
    super(data);
  }
}

export interface RoiCalculatorRelations {
  // describe navigational properties here
}

export type RoiCalculatorWithRelations = RoiCalculator & RoiCalculatorRelations;
