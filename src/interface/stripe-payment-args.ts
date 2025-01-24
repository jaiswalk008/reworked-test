import { Json } from "aws-sdk/clients/robomaker";


export interface stripePaymentArgs {
    totalAmount: number;
    metaData: JSON;
    noOfCredits: number;
    email: string;
}
