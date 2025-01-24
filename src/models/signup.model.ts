import {Entity, model, property} from '@loopback/repository';

@model()
export class Signup extends Entity {
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


  constructor(data?: Partial<Signup>) {
    super(data);
  }
}

export interface SignupRelations {
  // describe navigational properties here
}

export type SignupWithRelations = Signup & SignupRelations;
