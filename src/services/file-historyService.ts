import { CustomerRepository, FileHistoryRepository } from "../repositories";
import { writeFileSync } from "fs";
import { getFilterSort } from "../helper/filter-sort";
const jwt = require('jsonwebtoken');

export class FileHistoryService {

  static async getUsageStatsForMonth(monthNumber: number, yearNumber: number, customerRepository: CustomerRepository, fileHistoryRepository: FileHistoryRepository) {

    let results: any = [];
    const customerCollection = (customerRepository.dataSource.connector as any).collection("Customer")
    const customers = await customerCollection.aggregate([
      {
        '$match': {
          '$expr': {
            '$and': [
              {
                '$ne': [
                  '$email', null
                ]
              }, {
                '$ne': [
                  '$pricing_plan', null
                ]
              }
            ]
          }
        }
      }, {
        '$lookup': {
          'from': 'FileHistory',
          'let': {
            'email': '$email'
          },
          'pipeline': [
            {
              '$addFields': {
                'month': {
                  '$month': '$upload_date'
                },
                'year': {
                  '$year': '$upload_date'
                }
              }
            }, {
              '$match': {
                '$expr': {
                  '$and': [
                    {
                      '$eq': [
                        '$email', '$$email'
                      ]
                    }, {
                      '$eq': [
                        '$status', 7
                      ]
                    }, {
                      '$eq': [
                        '$month', monthNumber
                      ]
                    }, {
                      '$eq': [
                        '$year', yearNumber
                      ]
                    }
                  ]
                }
              }
            }, {
              '$group': {
                '_id': 'email',
                'sum': {
                  '$sum': {
                    '$toInt': '$record_count'
                  }
                }
              }
            }
          ],
          'as': 'creditsUsed'
        }
      }, {
        '$project': {
          'email': 1,
          'pricing_plan': 1,
          '_id': 0,
          'creditsUsed': 1
        }
      }
    ]).toArray()
    customers.forEach((customer: any) => {
      let totalCreditsOfCurrentMonth = (customer.creditsUsed[0] && customer.creditsUsed[0].sum) ? customer.creditsUsed[0].sum : 0;
      results.push({ email: customer.email, pricing_plan: customer?.pricing_plan?.plan, subscription_status: customer?.pricing_plan?.stripe_subscription_status, pause_ends_at: customer?.pricing_plan?.resume_paused_subscription_at, number_of_rows_used_in_month: totalCreditsOfCurrentMonth })
    })

    //  If we need to store results in csv file
    const csvString = [
      [
        "email",
        "pricing_plan",
        "subscription_status",
        "pause_ends_at",
        "number_of_rows_used_in_month"
      ],
      ...results.map((result: any) => [
        result.email,
        result.pricing_plan,
        result.subscription_status,
        result.pause_ends_at,
        result.number_of_rows_used_in_month
      ])
    ]
      .map(e => e.join(","))
      .join("\n");
    let filePath = __dirname + `/../../.sandbox/try-${monthNumber}.csv`
    try {
      writeFileSync(filePath, csvString)
    } catch (e) {
      console.log('Error occured while writing to csv file')
      filePath = ''
    }
    return filePath
  }

  static async getParentAccountReport(request: any, customerRepository: CustomerRepository, fileHistoryRepository: FileHistoryRepository, authHeader: any, exportCSV: boolean = false) {

    const token = authHeader.replace('Bearer ', '');
    const decodedToken: any = jwt.decode(token);
    // Extract the email from the token payload
    const loogedInEmail = decodedToken?.email;
    
    let response = {};
    let totalSavings = 0;
    let totalRowsProcessed = 0;
    let isAdmin = false;
    try {
      let where: any = {}
      let fileHistorywhere: any = {}
      let order: string = 'upload_date DESC'
  
      const email: any = request.query.email;
      const externalOrderId: any = request.query.external_order_id;
      const status: any = request.query.status;
      const startUploadDate: any = request.query.start_date;
      const endUploadDate: any = request.query.end_date;
      const sortName: any = request.query.sort_name;
      const parentEmail: any = request.query.parent_email;
      let page: any = request.query.page || 1;
      const filtertype = "v1"
      const type: any = request.query.type_of_sorting || '';
      let pageSize: any = request.query.page_size || 10; // Number of items per page
      if (exportCSV) {
        page = null;
        pageSize = null
      }

      const skip = (page - 1) * pageSize; // Skip items for pagination
  
      if (email) {
        where['email'] = email
      }
  
      if (parentEmail) {
        where['parent_email'] = parentEmail
      }
      if (status) {
        fileHistorywhere['status'] = status
      }
      if (externalOrderId) {
        fileHistorywhere['$or'] = [
          { 'external_order_id': { $exists: false } }, // Field does not exist
          { 'external_order_id': null },               // Field is null
          { 'external_order_id': '' }                  // Field is an empty string
        ];
      }
  
      const defaultValue = false;
      let allFileHistory;
      const filterSort = getFilterSort({ where, filtertype, sortName, type }, defaultValue);
      where = filterSort.where;
  
      if (filterSort.order !== '') {
        order = filterSort.order
      }
  
      // First query to fetch file history for child emails
      let customersEmails = await customerRepository.find({
        where,
        fields: ['email'],
      }).then((customers: any) => customers.map((ele: any) => ele.email));

      let loggedInCustomer = await customerRepository.findOne({
        where: {email: loogedInEmail},
        fields: ['role'],
      }).then((customer: any) => {
        isAdmin = customer?.role?.toLowerCase() == 'admin' ? true : false
      });

      
      let fileHistory = await fileHistoryRepository.find({
        where: {
          email: { inq: customersEmails },
          ...fileHistorywhere,
          ...(startUploadDate ? { upload_date: { between: [new Date(startUploadDate), new Date(endUploadDate)] } } : {}),
        },
        limit: pageSize,
        skip,
        order: [order],
        fields: ['email', 'record_count', 'status', 'upload_date', 'filename', 'external_order_id', 'id'],
      });
      
      where = {
        email: { inq: customersEmails }, // Use child emails here
      };
      // If no records are found, query using parent email
      if (parentEmail && fileHistory.length === 0) {
        where = {
          email: parentEmail
        };
        fileHistory = await fileHistoryRepository.find({
          where: {
            ...where, // Fallback to parent email
            ...fileHistorywhere,
            ...(startUploadDate ? { upload_date: { between: [new Date(startUploadDate), new Date(endUploadDate)] } } : {}),
          },
          limit: pageSize,
          skip,
          order: [order],
          fields: ['email', 'record_count', 'status', 'upload_date', 'filename', 'external_order_id', 'id'],
        });
      } 
      allFileHistory = await fileHistoryRepository.find({
        where: {
          ...where,
          ...fileHistorywhere,
          ...(startUploadDate ? { upload_date: { between: [new Date(startUploadDate), new Date(endUploadDate)] } } : {}),
        },
        fields: ['record_count', 'status', 'saving_amount', 'rows_below_100'],
      });
      const length = allFileHistory.length;
  
      // Calculate total savings and rows processed
      allFileHistory.map((filehistoryRecord: any) => {
        if (filehistoryRecord.status == 7) {
          filehistoryRecord.saving_amount = filehistoryRecord.saving_amount
            ? filehistoryRecord.saving_amount
            : (filehistoryRecord?.rows_below_100 || 0 * 0.67);
          let savingAmountFactor = filehistoryRecord.saving_amount;
          totalSavings += Number(savingAmountFactor) || 0;
          totalRowsProcessed += filehistoryRecord?.record_count ? Number(filehistoryRecord?.record_count) : 0;
        }
      });
  
      response = {
        totalSavings,
        totalRowsProcessed,
        totalFilesProcessed: length,
        totalLength: length,
        fileHistory: fileHistory
      }
      const timestamp = new Date().toISOString().replace(/[:.]/g, '-'); // Replace ':' and '.' with '-' to make it valid
      let filePath = `${__dirname}/../../.sandbox/Report-${timestamp}.csv`;
  
      if (exportCSV) {
        //  If we need to store results in csv file
        const csvString = [
          [
            "email",
            "record_count",
            ...(isAdmin ? ["status"] : []), // Conditionally add the status column
            "upload_date",
            "filename",
            "external_order_id"
          ],
          ...fileHistory.map((result: any) => [
            result.email,
            result.record_count,
            ...(isAdmin ? [result.status] : []), // Conditionally add the status value
            result.upload_date,
            result.filename,
            result.external_order_id
          ])
        ]
          .map(e => e.join(","))
          .join("\n");
  
        try {
          writeFileSync(filePath, csvString)
        } catch (e) {
          console.log('Error occured while writing to csv file')
          return { msg: e.message, data: {}, status: 500, filePath: null, exportCSV };
        }
      }
      return { msg: "Reports fetched successfully", data: response, status: 200, filePath, exportCSV };
    }
    catch (error) {
      console.log("Error in reports api", error.message);
      return { msg: error.message, data: {}, status: 500, filePath: null, exportCSV };
    }
  }
}