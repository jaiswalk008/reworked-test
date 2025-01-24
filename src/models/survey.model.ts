import { Entity, model, property } from "@loopback/repository";
@model()
export class Survey extends Entity {
  @property({
    type: "string",
    id: true,
    generated: true,
  })
  id?: string;

  @property({
    type: "string",
    required: false,
  })
  email: string;

  @property({
    type: "string",
    required: false,
  })
  name: string;

  @property({
    type: "string",
    required: false,
  })
  companyName: string;

  @property({
    type: "string",
    required: false,
  })
  phone: string;

  @property({
    type: "object",
    // itemType: 'object',
    required: false,
  })
  question_answers: any;

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

  constructor(data?: Partial<Survey>) {
    super(data);
  }
}

export interface SurveyRelations {
  // describe navigational properties here
}

export type SurveyWithRelations = Survey & SurveyRelations;
