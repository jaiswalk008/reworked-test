
import {
    BindingScope,
    config,
    ContextTags,
    injectable,
    Provider,
} from '@loopback/core';
import {
    Request,
} from "@loopback/rest";
import multer from 'multer';
import { FILE_UPLOAD_SERVICE } from '../keys';
import { FileUploadHandler } from '../types';
import fs from "fs";
import { FileHistoryRepository } from "../repositories";
import { Customer, Referral } from "../models";
import { Parser } from 'json2csv';
@injectable({
    scope: BindingScope.TRANSIENT,
    tags: { [ContextTags.KEY]: FILE_UPLOAD_SERVICE },
})
export class ReferralsService implements Provider<FileUploadHandler> {
    constructor(@config() private options: multer.Options = {}) {
        if (!this.options.storage) {
            // Default to in-memory storage
            this.options.storage = multer.memoryStorage();
        }
    }

    value(): FileUploadHandler {
        return multer(this.options).any();
    }

    static unLinkFileIfExists(filePath: string) {
        if (fs.existsSync(filePath)) {
            fs.unlinkSync(filePath)
        }
    }


    static async fetchReports(fileHistoryRepository: FileHistoryRepository, customerData: Customer[], referralData: Referral) {


        try {
            const referral_code = referralData.referral_code
            let matchObject: any = {
                email: { $in: customerData.map((ele: any) => ele.email) },
                status: 7,
            };

            const fileHistoryCollection = (fileHistoryRepository.dataSource.connector as any).collection("FileHistory");

            let fileHistorySumForDates = await fileHistoryCollection.aggregate([
                { $match: matchObject },
                { $group: { _id: "$email", sum: { $sum: { $toInt: "$record_count" } } } }
            ]).toArray();

            // Map the results to include pricing plan and other details
            const result: {
                name: any;
                email: any;
                pricingPlan: any;
                recordCount: any;
                referrer: string;
            }[] = [];
            customerData.map((customer: any) => {
                const fileHistory = fileHistorySumForDates.find((fh: any) => fh._id === customer.email);
                result.push({
                    name: customer.name,  // Assuming you have 'name' in customer data
                    email: customer.email,
                    pricingPlan: customer?.pricing_plan?.plan,  // Assuming you have 'pricing_plan' in customer data
                    recordCount: fileHistory ? fileHistory.sum : 0,
                    referrer: referral_code
                });
            });
            const fields = ['name', 'email', 'pricingPlan', 'recordCount', 'referrer'];
            const json2csvParser = new Parser({ fields });
            const csv = json2csvParser.parse(result);

            // Create the filename
            const filename = `referrals_${referral_code}.csv`;
            // Write the CSV to a file
            fs.writeFileSync(filename, csv);
            const fileContent = fs.readFileSync(filename).toString('base64');

            if (fs.existsSync(filename)) {
                fs.unlinkSync(filename)
            }
            console.log("fileHistorySumForDates")
            return {
                msg: 'Referral report fetched successfully',
                data: {
                    base64: fileContent,
                    filename: filename
                },
                status: 200
            };
            

        } catch (error: any) {
            console.error("Error in referrals service", error);
            return {
                msg: 'Something went wrong', data: null, status: 400
            }
        }
    }
}
