import { addOns } from "../constant/add_ons";
import { industryTypes } from "../constant/industry_type";
import { perUnitCostAndRangeObj, calculateRowsLeftForUser, perUnitCostLeadGeneration, getFirstAndLastDateOfMonth, creditsUsedForDates, solarleadScoringCost } from "../helper";
import { AdminEventsRepository, CustomerIndustryRepository, FileHistoryRepository, PromoRepository, TransactionHistoryRepository } from "../repositories";
import { CustomerRepository } from "../repositories/customer.repository";
import stripeClient from "../services/stripeClient";
export class CustomerService {
  static async upsertRowCredits(
    adminEmail: string,
    email: string,
    newTotalRowCredits: number,
    customerRepository: CustomerRepository,
    fileHistoryRepository: FileHistoryRepository,
    adminEventsRepository: AdminEventsRepository,
    promoRepository: PromoRepository,
    transactionHistoryRepository: TransactionHistoryRepository,
    customerIndustryRepository: CustomerIndustryRepository
  ): Promise<[number, any]> {
    let customer = await customerRepository.findOne({
      // fields: ['id', 'email', 'investment_profile'],
      where: { email },
    });
    if (customer) {
      let x = await adminEventsRepository.create({
        admin: adminEmail,
        date: new Date(),
        user: email,
        num_credits: newTotalRowCredits - customer.row_credits,
      });
      // console.log("x = ",x)
      customer.row_credits = newTotalRowCredits;
      // console.log("updated customer = ",customer)
      await customerRepository.update(customer, { email: customer.email });
      // console.log(email);
      // console.log(customer?.row_credits);
      return this.getCustomerDetailsObj(
        email,
        customerRepository,
        fileHistoryRepository,
        promoRepository,
        transactionHistoryRepository,
        customerIndustryRepository
      );
    }
    return [500, { msg: "error in updating row credits" }];
    //call find customer detail fcn
  }
  static async upsertCostPerRow(
    adminEmail: string,
    email: string,
    newCostPerRow: number,
    leadGenNewCostPerRow: number,
    customerRepository: CustomerRepository,
    fileHistoryRepository: FileHistoryRepository,
    adminEventsRepository: AdminEventsRepository,
    promoRepository: PromoRepository,
    transactionHistoryRepository: TransactionHistoryRepository,
    customerIndustryRepository: CustomerIndustryRepository
  ): Promise<[number, any]> {
    let customer = await customerRepository.findOne({
      // fields: ['id', 'email', 'investment_profile'],
      where: { email },
    });
    if (customer) {
      let x = await adminEventsRepository.create({
        admin: adminEmail,
        date: new Date(),
        user: email,
        cost_per_row: newCostPerRow,
        num_credits: 0,
      });
      // console.log("x = ",x)
      if(leadGenNewCostPerRow){
        customer.lead_gen_per_unit_price = [{ costPerRow: leadGenNewCostPerRow, range: ["1000"] }]; //todo - update w correct range? if needed  
      }
      if(newCostPerRow)
        customer.per_unit_price = [{ costPerRow: newCostPerRow, range: ["1000"] }]; //todo - update w correct range? if needed
      // console.log("updated customer = ",customer)
      await customerRepository.update(customer, { email: customer.email });
      console.log(email);
      console.log(customer?.per_unit_price);
      return this.getCustomerDetailsObj(
        email,
        customerRepository,
        fileHistoryRepository,
        promoRepository,
        transactionHistoryRepository,
        customerIndustryRepository,
      );
    }
    return [500, { msg: "error in updating row credits" }];
    //call find customer detail fcn
  }

  static async getPerUnitPriceForCustomer(email: string, customerRepository: CustomerRepository, customerIndustryRepository: CustomerIndustryRepository) {
    const customer = await customerRepository.findOne({
      where: { email },
    });
    const customerIndustry = await customerIndustryRepository.findOne({
        where: { email },
      });

    let perUnitCostAndRange: object[] = [];
    let perUnitCostAndRangeLeadGeneration: object[] = [];
    if (customer && customerIndustry) {
      let industryType = customerIndustry.industry_type ;
      if (customer?.per_unit_price && customer.per_unit_price.length) {
        perUnitCostAndRange = customer.per_unit_price;
      } else if (customer?.pricing_plan?.stripe_subscription_status == "active") {
        const subscriptionPlan = customer?.pricing_plan.plan?.toLowerCase() || "";
        
        if(industryType === industryTypes.SOLAR_INSTALLER)
          perUnitCostAndRange = solarleadScoringCost[subscriptionPlan];
        else 
          perUnitCostAndRange = perUnitCostAndRangeObj[subscriptionPlan];
      }
      //if the customers has not subscribed to any plan and it does not belong to real estate industry then send the perUnitCoseRange
      else if(!customer?.per_unit_price && customerIndustry?.industry_type !== industryTypes.REAL_ESTATE_INVESTORS){
        perUnitCostAndRange = solarleadScoringCost["payasyougo"];
      }
      perUnitCostAndRangeLeadGeneration = perUnitCostLeadGeneration['payasyougo'];
      if (customer?.lead_gen_per_unit_price && customer.lead_gen_per_unit_price.length) {
        perUnitCostAndRangeLeadGeneration = customer.lead_gen_per_unit_price;
      }
      
      const responseToSend = {
        data: {
          perUnitCostAndRange,
          perUnitCostAndRangeLeadGeneration
        },
        msg: "Customized Plan fetched successfully",
        status: 200,
      };
      return responseToSend;
    } else {
      return { data: null, status: 400, msg: "Customer not found" };
    }
  }

  static async getCustomerDetailsObj(
    email: string,
    customerRepository: CustomerRepository,
    fileHistoryRepository: FileHistoryRepository,
    promoRepository: PromoRepository,
    transactionHistoryRepository: TransactionHistoryRepository,
    customerIndustryRepository: CustomerIndustryRepository,
    industryName?: string
  ): Promise<[number, any]> {
    const responseFromFuntion = await this.getPerUnitPriceForCustomer(email, customerRepository, customerIndustryRepository);
    let status = 200;
    let msg = "Customer data fetched successfully";
    let responseToSend = { data: {}, msg };
    let remainingRowsForMonth = 0;
    let remainingPurchasedCredits = 0;
    let results: any;
    let rowsUserForMonth = 0;
    let totalAllowedRowCount = 0;
    let customerData = {};
    let perUnitCostAndRange: object[] = [];
    let perUnitCostAndRangeLeadGeneration: object[] = [];
    let applicablePromo: any = [];
    let totalRowsProcessed = {};
    let totalSpend = {};
    let nextBillingDate = null;
    let daysLeftInBillingCycle = null;
    let findCustomerByEmail;
    try {
      if (responseFromFuntion && responseFromFuntion.data && responseFromFuntion.data.perUnitCostAndRange) {
        perUnitCostAndRange = responseFromFuntion.data.perUnitCostAndRange;
        
      }
      if (responseFromFuntion && responseFromFuntion.data && responseFromFuntion.data.perUnitCostAndRangeLeadGeneration) {
        perUnitCostAndRangeLeadGeneration = responseFromFuntion.data.perUnitCostAndRangeLeadGeneration
      }
      // TODO: Remove this hard-coded value
      if (!industryName) industryName = industryTypes.REAL_ESTATE_INVESTORS;
      
      if (!industryName) {
        findCustomerByEmail = await customerRepository.findOne({
          fields: [
            "email",
            "pricing_plan",
            "row_credits",
            "name",
            "custom_rows_per_month",
            // "investment_profile",
            "referrer",
            "isCouponRedeemed",
            "add_ons",
            // "industry_type",
            "role",
            "stripe_customer_id",
            "lead_gen_row_credits",
            "source",
            "parent_email",
            "survey_answer"
          ],
          where: { email: email },
        });
      } else {
        const customerCollection = (customerRepository.dataSource.connector as any).collection("Customer");
        const findCustomerByEmailAggregate = await customerCollection
          .aggregate([
            {
              $match: {
                email: email,
              },
            },
            {
              $lookup: {
                from: "CustomerIndustry",
                let: {
                  email: "$email",
                },
                pipeline: [
                  {
                    $match: {
                      $expr: {
                        $and: [
                          {
                            $eq: ["$email", "$$email"],
                          },
                          {
                            // $eq: ["$industry_type", industryName],
                          },
                        ],
                      },
                    },
                  },
                  {
                    $project: {
                      _id: 0,
                      email: 0,
                    },
                  },
                ],
                as: "industry_info",
              },
            },
            {
                $lookup: {
                    from: "CustomerIndustry",
                    let: {
                      email: "$email",
                    },
                    pipeline: [
                      {
                        $match: {
                          $expr: {
                            $and: [
                              {
                                $eq: ["$email", "$$email"],
                              },
                            ],
                          },
                        },
                      },
                      {
                        $project: {
                          _id: 0,
                          email: 0,
                          industry_profile: 0,
                        },
                      },
                    ],
                    as: "industry_type",
                },
            },
            {
                $project: {
                    email: 1,
                    pricing_plan: 1,
                    row_credits: 1,
                    name: 1,
                    custom_rows_per_month: 1,
                    // investment_profile: 1,
                    referrer: 1,
                    isCouponRedeemed: 1,
                    role: 1,
                    stripe_customer_id: 1,
                    lead_sorting_default_model: 1,
                    lead_gen_row_credits: 1,
                    source: 1,
                    parent_email:1,
                    survey_answer:1,
                    industry_info: 1,
                    add_ons: 1,
                    industry_type: {
                      $cond: {
                        if: { $or: [{ $eq: ["$industry_type", ""] }, { $eq: [{ $size: "$industry_type" }, 0] }] },
                        then: "unknown", // Or a different default value
                        else: { $arrayElemAt: ["$industry_type.industry_type", 0] },
                      },
                    },
                  },
            },
            {
              $unwind: {
                path: "$industry_info",
                preserveNullAndEmptyArrays: true,
              },
            },
            {
              $addFields: {
                lead_gen_row_credits: { $ifNull: ["$lead_gen_row_credits", 0] },
              },
            },
          ])
          .toArray();


        findCustomerByEmail = findCustomerByEmailAggregate[0] ? findCustomerByEmailAggregate[0] : null;
      }

      // if customer data then find allowed credits
      if (findCustomerByEmail) {
        if (findCustomerByEmail.pricing_plan) {
          let getPlan = findCustomerByEmail.pricing_plan.plan?.toUpperCase();
          let current_period_end = findCustomerByEmail.pricing_plan.current_period_end;
          if (findCustomerByEmail.pricing_plan.current_period_end == null) {
            current_period_end = 1680307200;
          }
          if (getPlan !== null && getPlan !== undefined) {
            let getApplicablePromosPromise = this.getApplicablePromos(getPlan, promoRepository);
            let getTotalRowsPromise = this.getTotalRows(email, fileHistoryRepository);
            let getTotalSpendPromise = this.getTotalSpend(email, transactionHistoryRepository);
            let getNextBillingDatePromise = this.getNextBillingDate(current_period_end);
            let getDaysLeftInBillingCyclePromise = this.getDaysLeftInBillingCycle(current_period_end);
            // let creditsUsedForDatesPromise = getPlan == 'POSTPAID' ? creditsUsedForDates(email, fileHistoryRepository, getFirstAndLastDateOfMonth(0).startDate, getFirstAndLastDateOfMonth(0).endDate) : Promise.resolve(null);
          
            try {
              [applicablePromo, totalRowsProcessed, totalSpend, nextBillingDate, daysLeftInBillingCycle] = await Promise.all([
                getApplicablePromosPromise,
                getTotalRowsPromise,
                getTotalSpendPromise,
                getNextBillingDatePromise,
                getDaysLeftInBillingCyclePromise,
                // creditsUsedForDatesPromise
              ]);
          
              
              if(getPlan == 'POSTPAID'){
                const { startDate, endDate } = getFirstAndLastDateOfMonth(0);
                const resultFromFunction = await creditsUsedForDates([email], fileHistoryRepository, startDate, endDate) ;
                rowsUserForMonth = resultFromFunction[email] || 0;
              }
            } catch (error) {
              console.error("An error occurred:", error);
            }
          }
        }
        const cus = { ...findCustomerByEmail };
        const { login_history, add_ons, ...customerResToSend } = cus;
        customerData = customerResToSend;
      }
      let calculateRowsLeftPromise = calculateRowsLeftForUser(findCustomerByEmail, fileHistoryRepository);
      [results] = await Promise.all([calculateRowsLeftPromise]);
      totalAllowedRowCount = results.totalAllowedRowCount;
      remainingRowsForMonth = results.remainingRowsForMonth;
      remainingPurchasedCredits = totalAllowedRowCount - remainingRowsForMonth;
    } catch (error) {
      responseToSend.msg = error.message;
      return [500, responseToSend];
    }

    responseToSend = {
      data: {
        ...customerData,
        totalCredits: remainingPurchasedCredits,
        totalCreditsForCurrentMonth: remainingRowsForMonth,
        perUnitCostAndRange,
        perUnitCostAndRangeLeadGeneration,
        applicablePromo,
        totalRowsProcessed,
        totalSpend,
        nextBillingDate,
        daysLeftInBillingCycle,
        rowsUserForMonth,
        lead_scoring_model_title: findCustomerByEmail?.add_ons?.[addOns.CUSTOM_MODEL]?.model_title
      },
      msg,
    };
    return [200, responseToSend];
  }

  static async isAdmin(email: string, customerRepository: CustomerRepository): Promise<boolean> {
    let adminData = await customerRepository.findOne({
      fields: ["email"],
      where: { role: "admin", email: email },
    });
    if (adminData) {
      return true;
    } else {
      return false;
    }
  }
  static async getApplicablePromos(plan: string, promoRepository: PromoRepository) {
    //search repository for applicable plan based on current  date, and plan name
    let promos = await promoRepository.find({
      // fields: ['id', 'email', 'investment_profile'],
      where: {
        applicable_pricing_plans: { inq: [[plan]] },
      },
    });

    return promos;
  }
  static async getTotalRows(email: string, fileHistoryRepository: FileHistoryRepository) {
    let allFiles = await fileHistoryRepository.find({
      where: {
        email: { inq: [email] },
      },
    });
    const sum = allFiles.reduce((sum, current) => +sum + +current.record_count, 0);
    return sum;
  }

  static async getTotalSpend(email: string, transactionHistoryRepository: TransactionHistoryRepository) {
    let allTransactions = await transactionHistoryRepository.find({
      where: {
        email: { inq: [email] },
      },
    });
    const sum = allTransactions.reduce((sum, current) => +sum + +current.invoice_amount, 0);
    return sum;
  }
  static async getNextBillingDate(current_period_end: any) {
    const period_end_date = new Date(current_period_end * 1000);
    const today = new Date();
    let end_date_str = new Intl.DateTimeFormat("en-US", { dateStyle: "long" }).format(period_end_date);
    return end_date_str;
  }

  static async getDaysLeftInBillingCycle(current_period_end: any) {
    const period_end_date = new Date(current_period_end * 1000);
    const today = new Date();
    let time_end_date = period_end_date.getTime();
    let time_today = today.getTime();
    let dif = time_end_date - time_today;
    dif = dif / (1000 * 60 * 60 * 24);
    //console.log("dif  = ", dif);
    return Math.ceil(dif);
  }

  static async create_stripe_customer(email: string): Promise<string | null> {
    try {
      const customer = await stripeClient.customers.create({
        email: email,
      });
      return customer.id;
    } catch (err) {
      console.log(err);
      console.log(`⚠️ Create Stripe Customer Failed.`);
    }
    return null;
  }
}
