import { CronJob, cronJob } from '@loopback/cron';
import { Where, repository } from '@loopback/repository';
import { AdminEventsRepository, CustomerIndustryRepository, CustomerRepository, FileHistoryRepository, TransactionHistoryRepository } from './repositories';
import { calculateRowsLeftForUser, getPriceFromRange, sendEmailToAdmin, getFirstAndLastDateOfMonth, creditsUsedForDates,
  calculateCreditUsageAccountWise,
  flattenDataForCSV,
  sendMailChimpEmail
 } from './helper';
import Process from 'process';
import { pricing_to_row_count } from './constant/pricing_plan';
import { Customer } from './models';
import { stripePayment } from './helper/stripe-payment';
import { CustomerService } from "./services/customer.service";
import { paymentTypes } from "./helper/constant";
import { sendTwit } from './helper/integrations';
const schedule = require('node-schedule');
import path from "path";
import fs from 'fs';
@cronJob()
export class RollOverCronJob extends CronJob {
  constructor(
    @repository(CustomerRepository) public customerRepository: CustomerRepository,
    @repository(FileHistoryRepository) public fileHistoryRepository: FileHistoryRepository,
    @repository(AdminEventsRepository) public adminEventsRepository: AdminEventsRepository,
    @repository(TransactionHistoryRepository) public transactionHistoryRepository: TransactionHistoryRepository,
    @repository(CustomerIndustryRepository) public customerIndustryRepository: CustomerIndustryRepository
  ) {
    super({
      name: 'my-job',
      onTick: () => {
        this.performRollOverCreditsCheck()
      },
      cronTime: '0 0 23 * * *', // Once every Every 1st of the month at 11:00:00 pm
      start: true,
    });

    // New job for additional task
    const postpaidBillingJob = new CronJob({
      name: 'post-paid-billing',
      onTick: () => {
        this.postpaidBilling()
      },
      cronTime: '0 11 1 * *', // Every 1st of the month at 11:00:00 am
      // cronTime: '*/2 * * * *', // Every 1st of the month at 11:00:00 am
      // cronTime: '0 11 * * *', // Every day at 11 am
      // cronTime: "* * * * *", // Run Every min
      start: true,
    });

    // New job for additional task
    const sendRandomTwit = new CronJob({
      name: 'sendRandomTwit',
      onTick: () => {
        this.sendRandomTwitFunction(5)
      },
      cronTime: "0 1 * * *", // Every day at 1 am
      // cronTime: "* * * * *", // Run Every min
      start: true,
    });
    const dormantUserEmailJob = new CronJob({
      name: 'dormant-user-email',
      onTick: () => {
        this.identifyAndEmailDormantUsers();
      },
      cronTime: '0 0 * * *', // Every day at midnight
      // cronTime: "* * * * *", // Run Every min
      start: true,
    });
    const subscriptionResumptionReminderJob  = new CronJob({
      name: 'paused-user-email',
      onTick: () => {
        this.identifyAndEmailResumptionUsers();
      },
      cronTime: '0 0 * * *', // Every day at midnight
      // cronTime: "* * * * *", // Run Every min
      start: true,
      
    });
 
  }

  async postpaidBilling() {

    const monthNames = [
      "January", "February", "March", "April", "May", "June",
      "July", "August", "September", "October", "November", "December"
    ];

    const minusMonth = 1; // to fetch dates for last month
    // const minusMonth = 0; // to fetch dates for current month
    const { startDate, endDate } = getFirstAndLastDateOfMonth(minusMonth);

    const month = monthNames[startDate.getMonth()];
    const year = startDate.getFullYear();

    const filter: Where<Customer> = {
      $or: [
        { 'pricing_plan.plan': 'postpaid' }, // Case-insensitive match
        { 'pricing_plan.plan': 'POSTPAID' } // Exact match
      ],
      // 'email': 'harshitkyal-test1@gmail.com'
    } as Where<Customer>;
    
    const postpaidCustomers = await this.customerRepository.find({ where: filter });

    const postpaidAutoDebitCustomers = postpaidCustomers.filter((ele: any) => {
      // Check if 'auto_debit' is not present or is true
      return ele?.pricing_plan?.auto_debit === undefined || ele?.pricing_plan?.auto_debit === null || ele?.pricing_plan?.auto_debit === true;
    });
    const postpaidInvoiceCustomers = postpaidCustomers.filter((ele:any)=> ele?.pricing_plan?.auto_debit == false);
    
    let postpaidAutoDebitCustomersWithCredit = await creditsUsedForDates(postpaidAutoDebitCustomers.map((ele:any)=> ele.email), this.fileHistoryRepository, startDate, endDate);
    let postpaidInvoiceCustomersWithCredit = await creditsUsedForDates(postpaidInvoiceCustomers.map((ele:any)=> ele.email), this.fileHistoryRepository, startDate, endDate);
    const usersAutoDebitConsolidatedCredits = await calculateCreditUsageAccountWise(postpaidAutoDebitCustomers, postpaidAutoDebitCustomersWithCredit)
    const usersInvoiceConsolidatedCredits = await calculateCreditUsageAccountWise(postpaidInvoiceCustomers, postpaidInvoiceCustomersWithCredit)
    
    // if auto debit is true then will auto debit from stripe
    for (const customerData of postpaidAutoDebitCustomers) {

        const email = customerData.email;
        console.log("POST PAID Auto Debit Billing with email", email)
        
        let totalCreditsOfLastMonth = usersAutoDebitConsolidatedCredits[email]?.totalCredits;
        if (totalCreditsOfLastMonth) {
            // totalCreditsOfLastMonth = 1000;
            const responseFromFunction = await CustomerService.getPerUnitPriceForCustomer(email, this.customerRepository, this.customerIndustryRepository);
            const sourceType = paymentTypes.POSTPAID_BILLING;
            if (customerData.row_credits >= totalCreditsOfLastMonth) {
                customerData.row_credits -= totalCreditsOfLastMonth;
            } else {
                if (responseFromFunction?.data?.perUnitCostAndRange && responseFromFunction?.data?.perUnitCostAndRange.length) {
                    // Calculate total amount based on extra credits
                    totalCreditsOfLastMonth = totalCreditsOfLastMonth - customerData.row_credits;
                    customerData.row_credits = 0;
                    
                    let totalAmount = await getPriceFromRange(totalCreditsOfLastMonth, responseFromFunction.data.perUnitCostAndRange);
                    // totalAmount = Math.round(totalAmount); // Round off to nearest integer

                    const metaData = {
                        "no_of_credits": totalCreditsOfLastMonth,
                        "total_cost": totalAmount,
                        "source_type": sourceType,
                        "leads_to_add": false,
                    }
                    console.log("POST PAID Billing with email, meta data", metaData)
                    const args = {
                        totalAmount, metaData, noOfCredits: totalCreditsOfLastMonth, email, payment_type: sourceType,
                        invoiceItemDescription: 'Post Paid Billing', leadsToAdd: false,
                        sourceType
                    };
                    const responseFromStripe = await stripePayment(args, customerData, this.customerRepository, this.transactionHistoryRepository, this.adminEventsRepository)
                    const dataFromStripe: any = responseFromStripe.data;

                    const msg = responseFromStripe.msg;
                    const statusCode = responseFromStripe.statusCode;
                    let optionsforAdminMail = null;
                    if (statusCode == 200) {
                        optionsforAdminMail = { content: `Invoice created for User ${customerData.email} and invoice is ${dataFromStripe.invoice_pdf}` }
                    } else {
                        optionsforAdminMail = { content: `Invoice failed for User ${customerData.email} and total amount was ${totalAmount}, reason: ${msg}` }
                    }
                    await sendEmailToAdmin('modelFilename', customerData, this.customerRepository, optionsforAdminMail)
                    console.log("msg from stripe", msg)

                    // emailSumMap.push({ email: customerData.email, sum: totalCreditsOfLastMonth });
                } else {
                    await this.transactionHistoryRepository.create({
                        email,
                        invoice_amount: NaN,
                        meta_data: {
                        "no_of_credits": totalCreditsOfLastMonth,
                        "total_cost": "NAN",
                        "source_type": sourceType,
                        "leads_to_add": false,
                        },
                        error: true,
                        error_detail: "Price is not setup for user",
                        payment_type: sourceType
                    });
                    const optionsforAdminMail = { content: `Invoice failed for User ${customerData.email}, total credits ${totalCreditsOfLastMonth} because of price not available in customer collection` }
                    await sendEmailToAdmin('modelFilename', customerData, this.customerRepository, optionsforAdminMail)
                }
            }
            await this.customerRepository.update(customerData);
        }
    }

    const attachments =[];
    if(postpaidInvoiceCustomers && postpaidInvoiceCustomers.length){
      const csvData = flattenDataForCSV(usersInvoiceConsolidatedCredits);
      // Create the filename
      const postpaidInvoiceFilename = `postpaid_invoice_${month}_${year}.csv`;
      // Write the CSV to a file
      fs.writeFileSync(postpaidInvoiceFilename, csvData);
      const fileContent = fs.readFileSync(postpaidInvoiceFilename).toString('base64');
      attachments.push(
        {
          type: 'text/csv', // MIME type for CSV files
          name: path.basename(postpaidInvoiceFilename),
          content: fileContent
        })
    }

    if(postpaidAutoDebitCustomers && postpaidAutoDebitCustomers.length){
      const csvData = flattenDataForCSV(usersAutoDebitConsolidatedCredits);
      // Create the filename
      const postpaidAutoDebitFilename = `postpaid_autodebit_${month}_${year}.csv`;
      // Write the CSV to a file
      fs.writeFileSync(postpaidAutoDebitFilename, csvData);
      const fileContent = fs.readFileSync(postpaidAutoDebitFilename).toString('base64');
      attachments.push(
        {
          type: 'text/csv', // MIME type for CSV files
          name: path.basename(postpaidAutoDebitFilename),
          content: fileContent
        })
    }
    const optionsforAdminMail = { content: `Attaching CSV for auto debit and invoice users. Please check`, attachments }
    sendEmailToAdmin('modelFilename', {name: "CronJob"}, this.customerRepository, optionsforAdminMail)
    // console.log("completed")
  };

  async performRollOverCreditsCheck() {
    // check if tomorrow is 1st of the month
    try {
      const today = new Date()
      const tomorrow = new Date(today)
      tomorrow.setDate(tomorrow.getDate() + 1)
      if (tomorrow.getDate() !== 1) return;
      const adminEmail = 'cron@gmail.com'
      const customerCollection = (this.customerRepository.dataSource.connector as any).collection("Customer")
      const activeCustomers = await customerCollection.aggregate([
        {
          '$match': {
            'pricing_plan': {
              '$ne': null
            },
            'pricing_plan.stripe_subscription_status': 'active',
            '$or': [
              {
                'subscription_log': null
              }, {
                '$expr': {
                  '$ne': [
                    {
                      '$month': {
                        '$getField': {
                          'field': 'timestamp',
                          'input': {
                            '$last': '$subscription_log'
                          }
                        }
                      }
                    }, today.getMonth()
                  ]
                }
              }
            ],
          }
        }
      ]).toArray()
      for (let i = 0; i < activeCustomers.length; i++) {
        const customer = activeCustomers[i]
        // if(customer.email !== '')
        let { totalAllowedRowCount, remainingRowsForMonth } = await calculateRowsLeftForUser(customer, this.fileHistoryRepository)
        if (isNaN(remainingRowsForMonth)) remainingRowsForMonth = 0
        const multiplier: number = parseInt(Process.env.MAX_ALLOWED_ROLL_OVER_CREDITS_MULTIPLIER as string)
        let pricingPlan = pricing_to_row_count[customer.pricing_plan.plan?.toUpperCase() as string]
        if (isNaN(pricingPlan)) pricingPlan = 0
        let maxAllowedCredits: number = pricingPlan * multiplier
        let rollOverCredits = customer.roll_over_credits ?? 0
        if (isNaN(rollOverCredits)) rollOverCredits = 0
        let rowCredits = customer.row_credits ?? 0
        if (isNaN(rowCredits)) rowCredits = 0
        const toAddCredits = Math.max(0, Math.min(maxAllowedCredits - rollOverCredits, remainingRowsForMonth))
        customer.roll_over_credits = rollOverCredits + toAddCredits
        customer.row_credits = rowCredits + toAddCredits
        await this.customerRepository.updateById(customer._id, customer)
        await this.adminEventsRepository.create({
          admin: adminEmail,
          user: customer.email,
          date: new Date(),
          num_credits: toAddCredits
        })
      }
      console.log('done cron job')
    } catch (e) {
      await this.adminEventsRepository.create({
        admin: 'cronError@gmail.com',
        user: 'cron',
        date: new Date(),
        num_credits: 0,
        remark: e.message
      })
    }
  }


  async sendRandomTwitFunction(numberOfTimes: number) {
    if(process.env.NODE_ENV != 'local'){
      const months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun', 'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec'];
      const currentTime = new Date();
      const endOfDay = new Date(currentTime);
      endOfDay.setHours(23, 59, 59, 999); // Set endOfDay to the last millisecond of the current day
      // const endOfDay = new Date(currentTime.getTime() + 2 * 60 * 1000); // Set endOfInterval to 2 minutes from the current time

      const remainingSecondsInDay = Math.floor((endOfDay.getTime() - currentTime.getTime()) / 1000);
      const formattedDate = `${currentTime.getDate()}-${months[currentTime.getMonth()]}-${currentTime.getFullYear()} ${currentTime.getHours()}:${currentTime.getMinutes()}:${currentTime.getSeconds()}`;
      console.log(`Current Time: ${formattedDate}`);

      const times = [];

      // Generate 5 random times within the next 24 hours
      // Generate numberOfTimes random times within the next 24 hours
      for (let i = 0; i < numberOfTimes; i++) {
        const randomSeconds = this.getRandomNumberInRange(1, remainingSecondsInDay); // Random seconds within the remaining time of the current day
        const randomTime = new Date(currentTime.getTime() + randomSeconds * 1000);
        const formattedRandomDate = `${randomTime.getDate()}-${months[randomTime.getMonth()]}-${randomTime.getFullYear()} ${randomTime.getHours()}:${randomTime.getMinutes()}:${randomTime.getSeconds()}`;
        times.push({ timeInSeconds: randomSeconds, date: randomTime, formattedDate: formattedRandomDate });
      }

      // Sort the times array based on timeInSeconds
      times.sort((a, b) => a.timeInSeconds - b.timeInSeconds);

      console.log(times);

      times.forEach((time, index) => {
        const { timeInSeconds, date, formattedDate } = time;
        const savingAmount = this.getRandomNumberInRange(300, 3000); // Random savings amount between 300 and 3000
        console.log(`Scheduling tweet ${index + 1} to trigger at ${formattedDate} with amount ${savingAmount}`);

        schedule.scheduleJob(date, () => {
          console.log(`Tweet ${index + 1} triggered at ${formattedDate} with amount ${savingAmount}`);
          sendTwit(savingAmount);
        });
      });
    };

  }
    getRandomNumberInRange(min: number, max: number) {
      return Math.floor(Math.random() * (max - min + 1)) + min;
    };

    private async identifyAndEmailDormantUsers() {
      try {
         const now = new Date();
        const sevenDaysAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);  
        const eightDaysAgo = new Date(now.getTime() - 8 * 24 * 60 * 60 * 1000);  
        const where: any = {
          pricing_plan: null,
        };
    
        // Step 1: Fetch customers with no 'pricing_plan'
        const dormantUsers: any[] = await this.customerRepository.find({
          where,
        });
        
        
        // Step 2: Filter users based on their last login
        for (const user of dormantUsers) {
           const loginHistory = Array.isArray(user.login_history) ? user.login_history : [];
          // Get the latest login time from login_history
          const latestLogin = loginHistory.length > 0
            ? new Date(loginHistory[loginHistory.length - 1].last_login) // Get the last login directly
            : null; // Default to null if there's no login history
     
          // Check if the latest login is older than 7 days
          if (latestLogin && latestLogin <= eightDaysAgo && latestLogin >= sevenDaysAgo) {
        
            // Check if the user has an associated industry profile
            const customerIndustry: any = await this.customerIndustryRepository.findOne({
              where: {
                email: user.email,  
              },
            });
    
            // Case 1: If the user has completed the industry profile but hasn't purchased a plan
            if (customerIndustry && customerIndustry.industry_profile && !user.pricing_plan) {
              // Send email to the user for completing the industry profile but not purchasing a plan
              sendMailChimpEmail("Dormant_User_Ver_1" , user.email,"",user.name);
              console.log(`Email sent to user who completed industry profile but has no plan: ${user.email}`);
            }
    
            // Case 2: If the user has NOT completed the industry profile and hasn't purchased a plan
            if (!customerIndustry || !customerIndustry.industry_profile) {
              // Send email to the user for not completing the industry profile and not purchasing a plan
              sendMailChimpEmail("Dormant_User_Ver_1" , user.email,"",user.name);
              console.log(`Email sent to user who hasn't completed industry profile: ${user.email}`);
            }
          }
        }
      } catch (error) {
        console.error("Error identifying or emailing dormant users:", error);
      }
    }
    private async identifyAndEmailResumptionUsers() {
      try {
        const now = new Date();
        const sixDaysFromNow = new Date(now.getTime() + 6 * 24 * 60 * 60 * 1000);
        const sevenDaysFromNow = new Date(now.getTime() + 7 * 24 * 60 * 60 * 1000);
       
        const where: any = {
          $and: [
            { 'pricing_plan.resume_paused_subscription_at': { $gte: sixDaysFromNow } },
            { 'pricing_plan.resume_paused_subscription_at': { $lt: sevenDaysFromNow } },
            { 'pricing_plan.resume_paused_subscription_at': { $ne:null } },

          ]
        };
        
         const usersToNotify = await this.customerRepository.find(
          {where}
        );
        // Send reminder emails to the filtered users
        for (const user of usersToNotify) {
          if (user.email) {
            let options = {
              resume_subsrciption_at:user.pricing_plan?.resume_paused_subscription_at?.toLocaleDateString()
            }
            sendMailChimpEmail("Coming_Off_Pause_Ver_1", user.email, "", user.name,false,options);
            console.log(`Email sent to user for subscription resumption: ${user.email}`);
          }
        }
      } catch (error) {
        console.error("Error identifying or emailing users for subscription resumption:", error);
      }
    }
    

    
}
