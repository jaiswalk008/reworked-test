import { Entity, model, property } from "@loopback/repository";
// import { InvestmentProfile } from '../types/investment_profile';
import { IndustryProfile } from "../types/industry_profile";
import { fileHistoryMetaData } from "../types/file_history_meta_data";
import { ConfidenceInsights } from "../types/confidence_insights";

@model()
export class FileHistory extends Entity {
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
    filename: string;

    @property({
        type: 'string',
        required: false
    })
    existing_customer_filename: string;

    @property({
        type: 'date',
        required: true
    })
    upload_date: Date;

    @property({
        type: 'string',
        required: true
    })
    record_count: number;

    @property({
        type: 'number',
        required: true
    })
    status: number;

    @property({
        type: 'string',
        required: false
    })
    error?: string;

    @property({
        type: 'string',
        required: false
    })
    error_detail?: string;

    @property({
        type: 'number',
        required: false
    })
    rows_below_100?: number;

    @property({
        type: 'string',
        required: true
    })
    file_extension: string;

    @property({
        type: 'object'
    })
    mapped_cols: object

    @property({
        type: 'object',
        required: false,
    })
    industry_profile: IndustryProfile

    // @property({
    //     type: 'object',
    //     required: false,
    // })
    // investment_profile: InvestmentProfile

    @property({
        type: 'string',
        required: false,
    })
    model_name: string;

    @property({
        type: 'string',
        required: false,
    })
    process_file_url: string;

    @property({
        type: 'string',
        required: false,
    })
    input_type: string;

    @property({
        type: 'string',
        required: false,
    })
    input_data_hash: string;

    @property({
        type: 'object',
        required: false,
    })
    meta_data: fileHistoryMetaData;

    @property({
        type: 'date',
        required: false
    })
    completion_date: Date;

    @property({
        type: 'object',
        required: false,
    })
    confidence_insights: ConfidenceInsights
    
    @property({
        type: 'string',
        required: false,
        default: null
    })
    source: string;

    @property({
        type: 'number',
        required: false,
        default: null
    })
    saving_amount: number;
    
    @property({
        type:'string',
        required:false
    })
    external_order_id:string

    constructor(data?: Partial<FileHistory>) {
        super(data);
    }
}

export interface FileHistoryRelations {
    // describe navigational properties here
}

export type FileHistoryWithRelations = FileHistory & FileHistoryRelations;
