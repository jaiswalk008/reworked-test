import { Entity, model, property } from '@loopback/repository';
import { integrationsMetaData } from "../types/integrations_meta_data";

@model()
export class Integrations extends Entity {
  @property({
    type: 'string',
    id: true,
    generated: true,
  })
  id?: string;

  @property({
    type: 'object',
    required: false,
  })
  metadata: integrationsMetaData;

  @property({
    type: 'string',
    required: true,
  })
  platform: string;

  @property({
    type: 'string',
    required: false,
  })
  code: string;

  @property({
    type: 'string',
    required: true,
  })
  email: string;

  @property({
    type: 'string',
    required: false,
  })
  access_token: string;

  @property({
    type: 'date',
    required: false,
  })
  access_token_expires_at: Date;

  @property({
    type: 'string',
    required: false,
  })
  refresh_token: string;

  @property({
    type: 'string',
    required: false,
  })
  token_type: string;

  @property({
    type:'object',
    required:false
  })
  column_mapping:object

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
  
  constructor(data?: Partial<Integrations>) {
    super(data);
  }
}

export interface IntegrationsRelations {
  // describe navigational properties here
}

export type IntegrationsRelationWithRelations = Integrations & IntegrationsRelations;
