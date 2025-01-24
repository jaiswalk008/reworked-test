import { Entity, model, property } from "@loopback/repository";
@model()
export class AdminEvents extends Entity {
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
  admin: string;
  @property({
    type: "date",
    required: true,
  })
  date: Date;
  @property({
    type: "string",
    required: true,
  })
  user: string;
  @property({
    type: "number",
    required: false,
  })
  num_credits: number;
  @property({
    type: "number",
  })
  cost_per_row: number;
  @property({
    type: "string",
  })
  remark: string;

  constructor(data?: Partial<AdminEvents>) {
    super(data);
  }
}

export interface AdminEventsRelations {
  // describe navigational properties here
}

export type AdminEventsWithRelations = AdminEvents & AdminEventsRelations;
