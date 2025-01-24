import { pricing_to_row_count } from './constant/pricing_plan';
import { CustomerRepository, FileHistoryRepository, CustomerModelsRepository, GenerateLeadsRepository } from "./repositories";
import { Customer } from './models';
import axios from 'axios';
import fs from "fs";
import { queue } from './helper/queue'
import { DbDataSource } from './datasources';
import path from "path";
const mailchimpClient = require("@mailchimp/mailchimp_transactional")(process.env.MAILCHIMP_KEY);
import { UploadS3, downloadFileFromS3IfNotAvailable, generatePresignedS3Url } from "./services";
import Papa from 'papaparse'; // Import the papaparse library
import { Blob } from 'buffer';
const crypto = require('crypto');
const auth0Domain = process.env.AUTH0_DOMAIN;
const clientId = process.env.AUTH0_CLIENT_ID || '';
const clientSecret = process.env.AUTH0_CLIENT_SECRET || '';
const audience = process.env.AUTH0_AUDIENCE || '';

const createCsvWriter = require('csv-writer').createObjectCsvWriter;
import { Parser } from 'json2csv';

export async function sendMailChimpEmail(template: string, email_to: string, filename: string, customer_name: string, isAdmin: boolean = false, options: { [key: string]: any } = {}) {
  
  options.filename = filename;
  options.customer_name = customer_name ? customer_name.split(' ')[0] : "";
  let { attachments, ...remainingOptions} = options;
  
  // Documentation here: https://mailchimp.com/developer/transactional/api/messages/send-using-message-template/
  // if (process.env.NODE_ENV == 'local') return;

  const global_merge_vars = Object.entries(remainingOptions).map(([name, content]) => ({ name, content }));
  const emailTo = email_to.split(',').map(email => ({ email, type: 'to' }));

  console.log("global_merge_vars", global_merge_vars)
  const response = await mailchimpClient.messages.sendTemplate({
    template_name: template,
    template_content: [{}],
    message: {
      "global_merge_vars": global_merge_vars,
      to: emailTo,
      attachments
    },
  });
  console.log(`Email sent ${JSON.stringify(response)}`)
  return response;

}

export const creditsUsedforCurrentBillingCycle = async (fileHistoryRepository: any, findbyemail: any, yearIndex?: number) => {
  const email = findbyemail.email;
  const todaysDate = new Date();
  let monthIndex = todaysDate.getMonth();

  const subscriptionStartDay = new Date(findbyemail.pricing_plan.current_period_end * 1000).getDate(); //   
  let todaysDay = todaysDate.getDate();
  if (!yearIndex) yearIndex = todaysDate.getFullYear();
  if (todaysDay < subscriptionStartDay) {
    if (monthIndex === 0) {
      monthIndex = 11; // Last month of previous year
      yearIndex = yearIndex - 1; // Previous year
    } else {
      monthIndex--; // Decrease month
    }
  }

  let matchObject: any = {
    email,
    status: 7,
    upload_date: {
      $gte: new Date(yearIndex, monthIndex, subscriptionStartDay), // Start date: First day of the month
      $lte: todaysDate, // Current date
    },
  };

  const fileHistoryCollection = (fileHistoryRepository.dataSource.connector as any).collection("FileHistory");

  let fileHistorySumThisMonth = await fileHistoryCollection.aggregate([
    { $addFields: { month: { $month: "$upload_date" }, year: { $year: "$upload_date" } } },
    { $match: matchObject },
    { $group: { _id: email, sum: { $sum: { $toInt: "$record_count" } } } }
  ]).toArray();

  let totalCreditsOfCurrentMonth = (fileHistorySumThisMonth[0] && fileHistorySumThisMonth[0].sum) ? fileHistorySumThisMonth[0].sum : 0;
  return totalCreditsOfCurrentMonth;
};


export const calculateRowsLeftForUser = async (findbyemail: any, fileHistoryRepository: FileHistoryRepository) => {
  const getPlan = findbyemail?.pricing_plan?.plan?.toUpperCase();
  let totalAllowedRowCount = 0;
  let remainingRowsForMonth = 0;
  let row_credits = findbyemail?.row_credits ? findbyemail?.row_credits : 0
  if (getPlan !== null && getPlan !== undefined) {
    if (getPlan == 'PAYASYOUGO') {
      totalAllowedRowCount = row_credits;
    }
    else {
      const todaysDate = new Date();

      let getAllowedRowLimitForMonth = 0;

      let fileHistoryRecordCountThisMonth = 0;
      if (findbyemail.pricing_plan.stripe_subscription_status === 'active') {
        if (getPlan == 'CUSTOM') {
          getAllowedRowLimitForMonth = findbyemail.custom_rows_per_month ? findbyemail.custom_rows_per_month : 0;
        }
        else {
          getAllowedRowLimitForMonth = pricing_to_row_count[getPlan];
        }
        fileHistoryRecordCountThisMonth = await creditsUsedforCurrentBillingCycle(fileHistoryRepository, findbyemail);
      }

      let ifPauseInSameMonth = true;
      const resumePauseSubsriptionAt = findbyemail.pricing_plan.resume_paused_subscription_at;
      const startPauseSubsriptionAt = findbyemail.pricing_plan.start_paused_subscription_at;
      if (resumePauseSubsriptionAt && resumePauseSubsriptionAt.getTime() > todaysDate.getTime()) {
        if (startPauseSubsriptionAt.getMonth() !== todaysDate.getMonth())
          ifPauseInSameMonth = false;
      }
      // if they've passed their subscription usage
      if (fileHistoryRecordCountThisMonth >= getAllowedRowLimitForMonth || !ifPauseInSameMonth) {
        totalAllowedRowCount = row_credits;
      }
      else {
        remainingRowsForMonth = getAllowedRowLimitForMonth - fileHistoryRecordCountThisMonth;
        totalAllowedRowCount = remainingRowsForMonth + row_credits;
      }
    }

  } else {
    totalAllowedRowCount = row_credits;
  }
  return { totalAllowedRowCount, remainingRowsForMonth };

}

export const sendEmailToAdmin = async (filename: string, { name, email }: any, customerRepository: CustomerRepository | null = null, options: { [key: string]: any } = {}) => {
  const { error, errorDetails, content: contentFromOption, attachments } = options;
  const contentToSend = contentFromOption || `Filename: ${filename} of customer: ${name} with email: ${email} has failed due to ${error} with error detail (${errorDetails}), please check and review.`;

  let adminEmail: string[];
  switch (process.env.NODE_ENV) {
    case 'development':
      adminEmail = ['shyam@reworked.ai', 'harshitkyal@gmail.com',"gaurav@reworked.ai","karan.jaiswal@vlo.city"];
      break;
    case 'local':
      adminEmail = ['harshitkyal@gmail.com','karan.jaiswal@vlo.city'];
      break;
    default:
      adminEmail = ['admin@reworked.ai', 'shyam@reworked.ai', 'pramod@reworked.ai',"gaurav@reworked.ai"];
  }
  // if (customerRepository) {
  //   adminsData = await customerRepository.find({
  //     fields: ['email'],
  //     where: { role: 'admin' },
  //   });
  //   adminEmail = adminsData && adminsData.length ? adminsData.map((ele: { email: any }) => ele.email) : [];
  // } 
  const adminEmailString = adminEmail.join(",");
  console.log(`${error}, emails sent with ${contentToSend} to ${adminEmail.join(",")}`);
  sendMailChimpEmail("admintemplate", adminEmailString, filename, name, true, { "admin_content": contentToSend, attachments });

}
export const solarleadScoringCost: perUnitCostAndRangeObj = {
  'payasyougo': [
    {
      costPerRow: 0.45,
      range: [1000, 3200],
    },
    {
      costPerRow: 0.40,
      range: [3300, 9000],
    },
    {
      costPerRow: 0.35,
      range: [9100, 24000],
    },
    {
      costPerRow: 0.30,
      range: [24100],
    },
  ],
};

export const perUnitCostAndRangeObj: perUnitCostAndRangeObj = {
  'payasyougo': [
    {
      costPerRow: 0.065,
      range: [1000, 3200],
    },
    {
      costPerRow: 0.055,
      range: [3300, 9000],
    },
    {
      costPerRow: 0.045,
      range: [9100, 24000],
    },
    {
      costPerRow: 0.035,
      range: [24100],
    },
  ],
  'solo': [
    {
      costPerRow: 0.0483,
      range: [1000],
    },
  ],
  'pro': [
    {
      costPerRow: 0.0361,
      range: [1000],
    },
  ],
  'enterprise': [
    {
      costPerRow: 0.0262,
      range: [1000],
    },
  ],
};

export const perUnitCostLeadGeneration: perUnitCostAndRangeObj = {
  'payasyougo': [
    // {
    //   costPerRow: 0.50,
    //   range: [0, 900],
    // },
    {
      costPerRow: 0.45,
      range: [1000, 3200],
    },
    {
      costPerRow: 0.40,
      range: [3300, 9000],
    },
    {
      costPerRow: 0.35,
      range: [9100, 24000],
    },
    {
      costPerRow: 0.30,
      range: [24100],
    },
  ],
  'solo': [
    {
      costPerRow: 0.0483,
      range: [1000],
    },
  ],
  'pro': [
    {
      costPerRow: 0.0361,
      range: [1000],
    },
  ],
  'enterprise': [
    {
      costPerRow: 0.0262,
      range: [1000],
    },
  ],
};

// Function to download the CSV file
export const downloadCSV = async (csvURL: string, fileName: string, email: string) => {
  try {
    let file = path.join(__dirname, `../.sandbox/${fileName}`); // Path to and name of object. For example '../myFiles/index.js'.
    const response = await axios.get(csvURL, { responseType: 'stream' });


    // const filePath = 'downloaded.csv';
    // file = 'downloaded.csv';
    const writeStream = fs.createWriteStream(file);
    response.data.pipe(writeStream);

    return new Promise((resolve, reject) => {
      writeStream.on('finish', () => resolve(file));
      writeStream.on('error', reject);
    });

  } catch (error) {
    console.error('Error downloading CSV:', error);
    throw error;
  }
}

type perUnitCostAndRangeObj = {
  [key: string]: {
    costPerRow: number;
    range: number[];
  }[];
};

export const pollCSVAvailability = async (job: any) => {
  const { csvUrl: url, generateLeadsRepository, retryCount = 0, retryLimit, objectIdToUpdate, customerName, customerEmail, retryFlag } = job?.data;
  try {

    const response = await axios.get(url);
    // const response = await axios.get('https://list.melissadata.com/ListOrderFiles/7125253_1205064941.csv');
    if (response.status === 200) {
      console.log('CSV is ready:', response.data);

      const generateLeadsModelData = await generateLeadsRepository.findById(objectIdToUpdate);

      if (generateLeadsModelData) {
        const email = generateLeadsModelData.email;

        generateLeadsModelData.status = 2;

        const timestamp = new Date().toISOString().replace(/[-:.]/g, '');
        const fileName = `leads_${timestamp}.csv`; // Add timestamp to the file name
        // download file and upload to s3
        let newFileUrl = path.join(__dirname, `../.sandbox/${fileName}`); // Path to and name of object. For example '../myFiles/index.js'.
        await downloadCSV(url, fileName, email);

        await cleanCSVFile(newFileUrl);
        let newfileStream = fs.createReadStream(newFileUrl);
        await UploadS3(newFileUrl, newfileStream, email);
        if (fs.existsSync(newFileUrl)) {
          fs.unlinkSync(newFileUrl);
        }
        await downloadFileFromS3IfNotAvailable(newFileUrl, email);
        generateLeadsModelData.file_name = fileName;
        const expireTime = undefined;
        const fileToDownload = generatePresignedS3Url(newFileUrl, email, expireTime)
        generateLeadsModelData.rwr_list_url = fileToDownload;
        await generateLeadsRepository.update(generateLeadsModelData);
        // Generate a random number between 3 and 7
        const randomMultiplier = Math.floor(Math.random() * 5) + 3;

        // Parse the lead_count to an integer (assuming generateLeadsModelData.lead_count is a string)
        const leadCount = parseInt(String(generateLeadsModelData.lead_count));

        // Calculate the result
        const result = leadCount * randomMultiplier;

        const options = {
          download_link: fileToDownload,
          region: generateLeadsModelData.place_list?.length 
          ? generateLeadsModelData.place_list.join(",") 
          : generateLeadsModelData.zip_codes?.length 
          ? generateLeadsModelData.zip_codes.join(",") 
          : "Nation Wide",
          generated_lead: generateLeadsModelData.lead_count,
          reviewed_lead: result
        }
        sendMailChimpEmail("generate_leads", email, fileName, customerName, true, options);
      }
      return { success: true };
    }
  }
  catch (error) {
    if (error.response && error.response.status === 404) {
      if (retryCount < retryLimit) {
        console.log(`CSV not yet ready for id ${objectIdToUpdate} and url ${url} after trying for ${retryCount} times, retrying  in 60 seconds...`);
        // Requeue the job with a delay of 60 seconds and increment retry count
        queue.add({ csvUrl: url, retryCount: retryCount + 1, retryLimit, objectIdToUpdate, customerName, customerEmail, retryFlag }, { delay: 60000 });
      } else {

        // if retry flag is true then dont sent email because admin is retrying to download content from melissa file
        if (!retryFlag) {
          const optionsforAdminMail = { content: `Issue with Melissa: Not able to download file with url ${url} of customer with email ${customerEmail}`, error: "Not able to download file from melissa" }
          sendEmailToAdmin('melissaFailure', { name: customerName, email: customerEmail }, null, optionsforAdminMail)
        }
        console.log(`CSV not ready after ${retryCount} multiple retries. Dropping job.`);
        return { success: false };
      }

    } else {
      console.error('Error checking CSV availability:', error.message);
      return { success: false, error_details: error }
    }
  }
}
  export const updateleadGenerationData = async (email:string,customerName:string,filename:string,downloadURL:string,errorObj:any={} ) =>{
    try{
      const dataSource = new DbDataSource(); 
      const generateLeadsRepository = new GenerateLeadsRepository(dataSource);

      const generateLeadsModelData = await generateLeadsRepository.findOne({where:{email , file_name:filename}});
      if(generateLeadsModelData){
        if(!Object.keys(errorObj).length){
          generateLeadsModelData.rwr_list_url = downloadURL;
          generateLeadsModelData.status = 2;
          await generateLeadsRepository.update(generateLeadsModelData)
          const randomMultiplier = Math.floor(Math.random() * 5) + 3;
          // Parse the lead_count to an integer (assuming generateLeadsModelData.lead_count is a string)
          const leadCount = parseInt(String(generateLeadsModelData.lead_count));
          const result = leadCount * randomMultiplier;
      
          const optionsForLeadgen = {
            download_link: generateLeadsModelData.rwr_list_url,
            region: generateLeadsModelData.place_list?.length 
          ? generateLeadsModelData.place_list.join(",") 
          : generateLeadsModelData.zip_codes?.length 
          ? generateLeadsModelData.zip_codes.join(",") 
          : "Nation Wide",
            generated_lead: leadCount,
            reviewed_lead: result,
          };
          sendMailChimpEmail("generate_leads", email, generateLeadsModelData.file_name, customerName, true, optionsForLeadgen);
        }
        else{
          generateLeadsModelData.error = errorObj?.msg || "Error in processing file";
          generateLeadsModelData.error_detail = errorObj?.details || "";
        }
        await generateLeadsRepository.update(generateLeadsModelData);
      }
      
   }
   catch(error){
     console.log("Error in updateleadGenerationData",error)
   }

    
  }
export const cleanCSVFile = async (pathToFile: string) => {

  // // Read and parse the CSV file
  const csvData = await fs.promises.readFile(pathToFile as string, 'utf-8');
  const { data: records, meta: { fields: headerFields } } = Papa.parse(csvData, { header: true });

  const fieldRenameMap: Record<string, string>  = {
    'MailAddr': 'mail_street_address', 
    'MailCity': 'mail_city',
    'MailState': 'mail_state_name_short_code'
  }
  // Columns to keep
  const columnsToKeep = ['MailAddr', 'MailCity', 'MailState', 'MailZip', 'MailPlus4', 'SiteAddr', 'SiteCity', 
    'SiteState', 'SiteZip', 'SitePlus4',  'FirstName1', 'LastName1', 'FullName1'];

  // Initialize modifiedHeaderFields
  let modifiedHeaderFields: string[] = [];

  // Remove phone number columns from headerFields
  if (headerFields) {
    modifiedHeaderFields = headerFields
      .filter(field => columnsToKeep.includes(field))
      .map(field => fieldRenameMap[field] || field); // Rename if mapping exists
  }

  const modifiedRecords = (records as Record<string, any>[]).map((record: Record<string, any>) => {
    const modifiedRecord: Record<string, any> = {};
    columnsToKeep.forEach(column => {
      const newColumn = fieldRenameMap[column] || column;
      modifiedRecord[newColumn] = record[column];
    });
    return modifiedRecord;
  });

  // Create the modified CSV string with header
  const modifiedCsvString = [modifiedHeaderFields.join(',')].concat(
    modifiedRecords.map(record => modifiedHeaderFields.map(field => record[field]).join(','))
  ).join('\n');

  // Write the modified CSV to a file
  await fs.promises.writeFile(pathToFile, modifiedCsvString);
}

const buildUrl = (baseUrl: string, queryParams: object) => {
  const url = new URL(baseUrl);

  for (const [key, value] of Object.entries(queryParams)) {
    url.searchParams.append(key, value);
  }
  return url.toString();
}
export const sendFileToCallback = async (fileHistoryRepository: FileHistoryRepository, filehistoryObj: any, options: any) => {

  try {
    let inputType = "url";
    const callbackUrl = filehistoryObj?.meta_data?.callback_url;
    if (callbackUrl) {
      const queryParamsObj = {
        // id: this.melissaApiKey
      }
      const apiUrl = buildUrl(callbackUrl, queryParamsObj);
      let objectToSend = {};
      if (filehistoryObj?.input_type) inputType = 'JSON';

      if (inputType == "JSON") {
        let processedData = await parseFileToJSON(`BETTY_${filehistoryObj.filename}`, filehistoryObj.email)
        objectToSend = { file_upload_identifier: filehistoryObj.id, processed_file_url: options.download_url, processed_data: processedData?.data?.parsedData, status: "PROCESSED" }
      }
      else objectToSend = { file_upload_identifier: filehistoryObj.id, processed_file_url: options.download_url, status: "PROCESSED" }

      axios.post(apiUrl, objectToSend)
        .then(response => {
          // Handle successful response here
          console.log(response.data); // Assuming you want to log the response data
        })
        .catch(error => {
          // Handle error here
          console.error('Error while calling callback url:', error.message);
        });

      // const dataSource = new DbDataSource(); // Replace with your actual data source instantiation
      // const fileHistoryRepository = new FileHistoryRepository(dataSource);
      filehistoryObj.meta_data = {
        ...filehistoryObj.meta_data,
        status: "PROCESSED"
      }
      await fileHistoryRepository.update(filehistoryObj);
      console.log("after file histroy update")

    }
  } catch (error) {
    console.error("Error in sendFileToCallback", error);
  }

}

export const generateApiKey = () => {
  const randomPart = crypto.randomBytes(32).toString('hex');

  // Insert dashes every 4 characters for better readability
  const formattedRandomPart = randomPart.replace(/(.{4})/g, '$1-').slice(0, -1);

  return formattedRandomPart;
}
async function checkUrlAccessibility(url: string) {
  try {
    // Send a HEAD request to the URL
    const { data, headers } = await axios.get(url, { responseType: 'blob' });
    // Check if the response data is empty or if it contains HTML
    if (!data && typeof data === 'string' && isHTML(data)) {
      return false; // If the data is empty or HTML, consider the file as inaccessible
    }
    // If the request is successful and the response data is not empty or HTML,
    // consider the URL as accessible
    return true;
  } catch (error) {
    // If an error occurs (e.g., network error, invalid URL), the URL is not accessible
    return false;
  }
}

// Function to check if a string contains HTML content
function isHTML(data: string) {
  // You can implement your own logic here to determine if the string contains HTML content
  // For example, you could check if it contains HTML tags such as "<html>", "<head>", or "<body>"
  // This is a basic example and may need to be adjusted based on your specific requirements
  return typeof data === 'string' && /<\s*html.*?>/i.test(data);
}
const getFileNameFromUrl = (url: string) => {
  const urlParts = url.split('/');
  return urlParts[urlParts.length - 1];
};

// Function to extract filename from Google Drive URL
const getFileNameFromDriveUrl = async (url: string) => {
  try {
    const response = await axios.head(url); // Send a HEAD request to get headers
    const contentDispositionHeader = response.headers['content-disposition'];

    if (contentDispositionHeader) {
      const matches = /filename="(.+?)"/.exec(contentDispositionHeader);
      if (matches && matches.length > 1) {
        return matches[1];
      }
    }
    return null; // Unable to retrieve filename
  } catch (error) {
    console.error('Error fetching original filename from Google Drive:', error.message);
    return null; // Return null in case of an error
  }
};

export const getOriginalFileName = async (url: string) => {
  try {

    const isAccessible = await checkUrlAccessibility(url);
    if (!isAccessible) {
      return { orgFileName: '', errorFlag: true, errorType: "url_access_issue" };
    }

    let fileName: any = null;

    if (url.includes('drive.google.com')) {
      fileName = await getFileNameFromDriveUrl(url); // Extract filename from Google Drive URL
    } else {
      fileName = getFileNameFromUrl(url); // Extract filename from regular URL
    }

    if (!fileName) {
      return { orgFileName: '', errorFlag: true, errorType: "url_access_issue" };
    }
    else {
      return { orgFileName: fileName, errorFlag: false };
    }
  } catch (error) {
    console.error('Error fetching original filename:', error.message);
    return { orgFileName: '', errorFlag: true, errorType: "url_access_issue" };
  }
}

export const getPriceFromRange = (rowCredits: number, priceRange: any) => {
  const pricing = priceRange;
  let amount = 0;
  let finalAmount = 0;
  // handle for rowCredits if less than 1000
  if (rowCredits < 1000) {
    const costPerRow = pricing[0].costPerRow;
    finalAmount = rowCredits * costPerRow;
  } else {
    for (const price of pricing) {
      const range = price.range;
      if (range.length < 2 && rowCredits >= range[0]) amount = price.costPerRow;
      else if (rowCredits >= range[0] && rowCredits <= range[1]) {
        amount = price.costPerRow;
      }
    }
    if (pricing.length > 0) {
      finalAmount = Math.round((rowCredits * amount * 100) / 100)
    }
  }

  return finalAmount
}

export const getOauthToken = async () => {
  try {

    const response = await axios.post(
      `${auth0Domain}/oauth/token`,
      new URLSearchParams({
        grant_type: 'client_credentials',
        audience: audience,
      }),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
        },
      }
    );
    console.log('Access Token:', response.data.access_token);
    return response;
  } catch (error) {
    console.error('Error getting token:', error.response ? error.response.data : error.message);
  }
};


export const checkOauthUser = async (email: string, password: string) => {

  try {
    const response = await axios.post(
      `${auth0Domain}/oauth/token`,
      new URLSearchParams({
        grant_type: 'password',
        username: email,
        password: password,
        audience: audience, // Your Auth0 API Identifier
        scope: 'openid profile email', // Define scopes as needed
        client_id: clientId, // Your Auth0 Client ID
        client_secret: clientSecret,
      }),
      {
        headers: {
          'Content-Type': 'application/x-www-form-urlencoded',
          Authorization: `Basic ${Buffer.from(`${clientId}:${clientSecret}`).toString('base64')}`,
        },
      }
    );
    return { statusCode: response?.status, msg: 'User Authenticated Successfully', data: null }
  } catch (error) {
    console.error('Error during login:', error);
    return { statusCode: 403, msg: "Password is not correct", data: error }
  }
}


export const getFirstAndLastDateOfMonth = (minusMonth: number) => {

  let yearIndex;
  const todaysDate = new Date();
  let monthIndex = todaysDate.getMonth();

  // Determine the year and month for the previous month

  if (monthIndex === 0) {
    monthIndex = 12 - minusMonth;
    yearIndex = todaysDate.getFullYear() - 1;
  } else {
    monthIndex = monthIndex - minusMonth;
    yearIndex = todaysDate.getFullYear();
  }
  // Calculate start and end dates for the previous month
  const startDate = new Date(yearIndex, monthIndex, 1); // First day of the previous month
  const endDate = new Date(yearIndex, monthIndex + 1, 0); // Last day of the previous monthIndex
  return { startDate, endDate }
}

export const creditsUsedForDates = async (emails: string[], fileHistoryRepository: FileHistoryRepository, startDate: Date, endDate: Date) => {
  let matchObject: any = {
    email: { $in: emails },
    status: 7,
    completion_date: {
      $gte: startDate, // Start date of the previous month
      $lte: endDate,   // End date of the previous month
    },
  };

  const fileHistoryCollection = (fileHistoryRepository.dataSource.connector as any).collection("FileHistory");

  let fileHistorySumForDates = await fileHistoryCollection.aggregate([
    { $match: matchObject },
    { $group: { _id: "$email", sum: { $sum: { $toInt: "$record_count" } } } }
  ]).toArray();

  // Create a result object to map each email to its corresponding sum
  let result:any = {};
  fileHistorySumForDates.forEach((item: any) => {
    result[item._id] = item.sum;
  });

  return result;
};


export const calculateCreditUsageAccountWise = async (customers: Customer[], fileHistory: any) => {
  const usageMap: any = {};

  customers.forEach((customer: Customer) => {
    usageMap[customer.email] = { totalCredits: 0, children: [] };
  });

  Object.keys(fileHistory).forEach((record: any) => {
    if (usageMap[record]) {
      usageMap[record].totalCredits += fileHistory[record];
    }
  });

  customers.forEach((customer: Customer) => {
    if (customer.parent_email) {
      const parentEmail = customer.parent_email;
      if (usageMap[parentEmail]) {
        usageMap[parentEmail].children.push({
          email: customer.email,
          totalCredits: usageMap[customer.email].totalCredits,
        });

        usageMap[parentEmail].totalCredits += usageMap[customer.email].totalCredits;
      }
    }
  });

  return usageMap;
}




export const createHash = async (jsonData: any) => {
  const jsonString = JSON.stringify(jsonData);

  // Create SHA-256 hash of the string
  const hash = crypto.createHash('sha256').update(jsonString).digest('hex');
  return hash;

}

export const parseFileToJSON = async (fileName: string, email: string) => {

  let newFileUrl = path.join(__dirname, `../.sandbox/${fileName}`);
  try {

    await downloadFileFromS3IfNotAvailable(newFileUrl, email);
    // Read the CSV file synchronously
    const fileContent = fs.readFileSync(newFileUrl, 'utf8');

    // Parse CSV content
    const results = Papa.parse(fileContent, {
      header: true,
      skipEmptyLines: true
    });

    // Assuming the results.data is an array of objects
    if (Array.isArray(results.data) && results.data.length > 0) {
      // Convert array of objects to array of arrays
      let columnHeaders: any = [];
      if (results.data.length > 0 && results.data[0] !== null && typeof results.data[0] === 'object') {
        columnHeaders = Object.keys(results.data[0]);
      }
      const dataArrays = results.data.map((obj: any) =>
        columnHeaders.map((header: any) => obj[header] ?? null)
      );

      // Prepend the column headers to the beginning of the data array
      const output = [columnHeaders, ...dataArrays];
      return { data: { parsedData: output }, msg: 'File Processed Successfully', success: true }
    } else {
      return { data: null, msg: 'Issue in parsing', success: false }
    }

  } catch (error) {
    console.error('An error occurred:', error);
    return { data: null, msg: error.message, success: false }
  } finally {
    if (fs.existsSync(newFileUrl)) {
      fs.unlinkSync(newFileUrl)
    }
  }

};

export const convertJsonToCsv = async (jsonData: any, csvFilePath: string) => {
  const header = jsonData[0];

  const csvWriter = createCsvWriter({
    path: csvFilePath,
    header: header.map((key: any, index: any) => ({ id: index.toString(), title: key })),
  });
  try {
    await csvWriter.writeRecords(jsonData.slice(1));
    console.log('CSV file written successfully');
  } catch (error) {
    console.error('Error writing CSV file:', error);
  }
}
export const checkforCustomBranding = (email: string) => {
  const customBrandCompany = {
    "reiprintmail": "luci"
  };
  const domainParts = email.split('@')[1].split('.').slice(-2).join('.');
  const matchedCompany = Object.entries(customBrandCompany).find(([companyDomain, branding]) => {
    return domainParts.includes(companyDomain);
  });

  if (matchedCompany) {
    return matchedCompany[1]; // Return the branding value
  } else {
    return "BETTY"; // Or any other default value if no match is found
  }
}

export const flattenDataForCSV = (usersInvoiceConsolidatedCredits: any) =>{

  interface Details {
    totalCredits: number;
    children: any[];
  }
  let flattenedData:any = [];
  let totalAllCredits = 0;

  // Flatten the data
  for (const [email, details] of Object.entries(usersInvoiceConsolidatedCredits) as [string, Details][]) {
    let totalCreditsForRow = details.totalCredits;

    
    const childData =[];
    let childCredits =0;
    // Add credits of children to the parent's row if any
    if (details.children.length > 0) {

      for (const child of details.children) {
        childCredits += child.totalCredits;
        // Push each child account
        childData.push({
          email: '',
          childAccount: child.email,
          totalCredits: child.totalCredits,
        });
      }

      // Add total row for parent including children
      childData.push({
        email: `${email} Total`,
        childAccount: '',
        totalCredits: details.totalCredits,
      });
    }

    // Push the parent email details
    flattenedData.push({
      email,
      childAccount: '',
      totalCredits: totalCreditsForRow - childCredits,
    });

    if(childData?.length) {
      flattenedData = flattenedData.concat(childData);
    }
    // Add the total credits (parent + children) to the overall total
    totalAllCredits +=  totalCreditsForRow;
  }

  flattenedData.push({
    email: 'Grand Total',
    childAccount: '',
    totalCredits: totalAllCredits,
  });

  const fields = ['email', 'childAccount', 'totalCredits'];
  const json2csvParser = new Parser({ fields });
  const csv = json2csvParser.parse(flattenedData);

  return csv;
}