import { Entity, model, property } from "@loopback/repository";
import { float } from "aws-sdk/clients/lightsail";

@model()
export class GenerateLeadsModel extends Entity {
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
    required: false,
  })
  model_name: string;
  @property({
    type: "string",
    required: false,
  })
  name: string;
  @property({
    type: "string",
    required: false,
  })
  file_name: string;
  @property({
    type: "number",
  })
  lead_count: number;

  @property({
    type: "number",
    default: 0,
  })
  amount_spent: float;

  @property({
    type: "array",
    itemType: "string",
  })
  place_list: string[];

  @property({
    type: "array",
    itemType: "string",
  })
  zip_codes: string[];

  @property({
    type: "number",
  })
  status: number;

  @property({
    type: "string",
  })
  rwr_list_url: string;

  @property({
    type: "string",
  })
  leads_provider_file: string;
  
  @property({
    type: "number",
  })
  rwr_count: number;


  @property({
    type: "string",
  })
  error_detail: string;
  
  @property({
    type: "string",
  })
  error: string;
  
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
    type: "boolean",
  })
  default_model: boolean;

  @property({
    type: "object",
  })
  leads_api_options: object;

  constructor(data?: Partial<GenerateLeadsModel>) {
    super(data);
  }
}

export interface GenerateLeadsRelations {
  // describe navigational properties here
}

export type GenerateLeadsWithRelations = GenerateLeadsModel & GenerateLeadsRelations;
