import {Entity, model, property} from "@loopback/repository";

interface MetaData {
    total_cost: number|string;
    no_of_credits: number,
    source_type: string,
    leads_to_add: boolean
}

@model()
export class TransactionHistory extends Entity {
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
        required: false
        })
    invoice_id: string;

    @property({
        type: 'string',
        required: false
    })
    invoice_url: string;

    @property({
        type: 'string',
        required: false
    })
    invoice_pdf: string;

    @property({
        type: 'object',
        itemType: 'any',
        required: true
    })
    meta_data: MetaData;

    @property({
        type: 'string',
        required: true
    })
    invoice_amount: number;

    @property({
        type: 'boolean',
        required: false
    })
    error?: boolean;

    @property({
        type: 'string',
        required: false
    })
    error_detail?: string;

    @property({
        type: 'string',
        required: false
    })
    payment_type?: string;


    @property({
        default: new Date(),
        type: 'date',
        required: false,
    })
    transaction_date?: Date;

    constructor(data?: Partial<TransactionHistory>) {
        super(data);
    }
}

export interface TransactionHistoryRelations {
    // describe navigational properties here
}

export type TransactionHistoryWithRelations = TransactionHistory & TransactionHistoryRelations;
