import { Entity, model, property } from "@loopback/repository";
 
@model()
export class Feedback extends Entity {
    @property({
        type: 'string',
        id: true,
        generated: true,
    })
    id?: string;

    @property({
        type: 'string',
        required: true
    })
    email: string;

    @property({
        type: 'string',
        required: true
    })
    title: string;

    @property({
        type: 'string',
        required: true
    })
    category: string;
    
    @property({
        type: 'string',
        required: true
    })
    feedback: string;

    @property({
        type: 'string',
        required: false
    })
    file_url: string;
    @property({
        type: 'string',
        enum: ["open","resolved"],
        required: true
    })
    status: string;
     

    @property({
        type: 'date',
        required: false
    })
    created_at: Date;
    @property({
        type: 'date',
        required: false
    })
    updated_at: Date;

    constructor(data?: Partial<Feedback>) {
        super(data);
    }
}

export interface FeedbackRelations {
}

export type FeedbackWithRelations = Feedback & FeedbackRelations;
