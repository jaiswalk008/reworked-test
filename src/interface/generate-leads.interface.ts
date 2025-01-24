import { Json } from "aws-sdk/clients/robomaker";


export interface GenerateLeadArgs {
    leadsCount: number;
    placeList: string[];
    nationWide: boolean;
    email: string;
    modelName: string;
    totalCost: number;
    stripePaymentMethodId: string;
    defaultModel?: boolean,
    zipCodes:string[];
}

