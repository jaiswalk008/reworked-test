// Uncomment these imports to begin using these cool features!

import { inject } from "@loopback/core";
import {
  get,
  HttpErrors,
  param,
  post,
  Request,
  requestBody,
  response,
  Response,
  RestBindings,
} from "@loopback/rest";
import { FILE_UPLOAD_SERVICE } from "../keys";
import {FileUploadHandler} from "../types";
import path from "path";
import fs from "fs";
import {CustomerIndustryRepository, CustomerRepository, FileHistoryRepository, TransactionHistoryRepository, AdminEventsRepository
  , IntegrationsRepository,GenerateLeadsRepository } from "../repositories";
import { downloadFileFromS3, FileUploadProvider, runPythonScript, UploadS3 } from "../services";
import { TokenServiceBindings, UserRepository } from '@loopback/authentication-jwt';
import { TokenService, authenticate } from '@loopback/authentication';

import { repository } from "@loopback/repository";
import {FileHistory} from "../models";
import { industryTypes } from "../constant/industry_type";
import { ScriptOutput } from "../constant/script_output";
import { sendEmailToAdmin } from '../helper';
import { extractDetailsFromAuthToken, extractPythonResponse } from "../helper/utils";
import { fileHistoryMetaData } from "../types/file_history_meta_data";
import { usageType } from "../constant/usage_type";

interface File {
  originalname: string;
  encoding: string;
  mimetype: string;
  buffer: Buffer;
  path: string;
}

/**
 * A controller to handle file uploads using multipart/form-data media type
 */
export class FileUploadController {
  /**
   * Constructor
   * @param customerRepository
   * @param fileHistoryRepository
   * @param handler - Inject an express request handler to deal with the request
   */
  constructor(
    @inject(TokenServiceBindings.TOKEN_SERVICE)
    public jwtService: TokenService,
    @repository(CustomerRepository)
    public customerRepository: CustomerRepository,
    @repository(FileHistoryRepository)
    public fileHistoryRepository: FileHistoryRepository,
    @repository(CustomerIndustryRepository)
    public customerIndustryRepository: CustomerIndustryRepository,
    @inject(FILE_UPLOAD_SERVICE) private handler: FileUploadHandler,
    @repository(TransactionHistoryRepository)
    public transactionHistoryRepository: TransactionHistoryRepository,
    @repository(AdminEventsRepository)
    public adminEventsRepository: AdminEventsRepository,
    @repository(IntegrationsRepository)
    public integrationsRepository: IntegrationsRepository,
    @repository(GenerateLeadsRepository)
    public generateLeadsRepository: GenerateLeadsRepository
  ) {}
    
  @authenticate('jwt')
  @post("/fileUpload", {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
            },
          },
        },
        description: "Files and fields",
      },
    },
  })
  async newFileUpload(
      @requestBody.file()
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
      @param.query.string('external_order_id') external_order_id?: string,
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        else {
          const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
          const customerDetails = await this.customerRepository.findOne({where:{email}});
          if(!customerDetails) {
            throw new Error("Customer not found");
          }
          const fileIdGenerate = customerDetails?.add_ons?.file_id_generate || false;
          const files = request.files as { [fieldname: string]: File[]; };
          const file = Array.isArray(files) ? files[0] : files;
          const filename = file.filename;
          const fileFormat: string = filename.split(".").pop()!;
          const filenameWithoutExtension: string = filename.split(".").slice(0,-1).join('.')
          const newFilename = `${filenameWithoutExtension}.csv`;
          const newPath = `${file.destination}/${filenameWithoutExtension}.csv`;

          let args = ["--file_path", file.path, "--file_format", fileFormat, "--output_path", newPath,"--file_id_generate",fileIdGenerate];
          const scriptPath = path.join(__dirname, '../../python_models/parseUploadFile.py');
          // Script parses uploaded xls and csv files and makes new output by removing completely empty rows and stores it into a csv file of the same name as uploaded name in .sandbox folder
          runPythonScript(scriptPath, args).then(async (python_output: any)=> {
            const { output } = await extractPythonResponse({ python_output });
            // let output: ScriptOutput = JSON.parse(python_output.at(-1).replace(/'/g, '"'));
            if (output.success !== 'True') {
              let file_history_obj = new FileHistory({
                email: request.headers.email as string,
                filename: newFilename,
                upload_date: new Date(),
                record_count: 0,
                status: 2,
                file_extension: ".csv",
                error_detail: output.error_details,
                error: output.error,
                external_order_id:external_order_id as string
              });
              file_history_obj = await this.fileHistoryRepository.create(file_history_obj)
              if (fs.existsSync(file.path)) {
                fs.unlinkSync(file.path);
              }
              return
            }
            const record_count = (output.mapped_cols as any).row_count
            if(fileFormat == 'xls' || fileFormat == 'xlsx')
              fs.unlinkSync(file.path);
            file.filename = newFilename;
            file.originalname = newFilename;
            file.path = newPath;
            resolve(
              FileUploadController.getNewFilesAndFields(
                  request,
                  this.customerRepository,
                  this.fileHistoryRepository,
                  this.customerIndustryRepository,
                  this.transactionHistoryRepository,
                  this.adminEventsRepository,
                  this.integrationsRepository,
                  this.generateLeadsRepository,
                  record_count, 
                  external_order_id || ''
              )
            );
          }).catch((e) => {
            reject(e)
            if (fs.existsSync(file.path)) {
              fs.unlinkSync(file.path);
            }
          })
          return response.send({
            success: true,
            message: "File has been uploaded successfully",
          })
        }
      });
    });
  }

  @authenticate('jwt')
  @post("/feedbackUpload", {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
            },
          },
        },
        description: "Upload Feedback",
      },
    },
  })
  async feedbackUpload(
      @requestBody.file()
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, (err: unknown) => {
        if (err) reject(err);
        else {
          resolve(
              FileUploadController.uploadFeedbackFile(
                  request,
                  this.customerRepository,
                  this.fileHistoryRepository
              )
          );
        }
      });
    });
  }

  /**
   * Upload feedback
   * @param request - Http request
   */
  private static async uploadFeedbackFile(
    request: Request,
    customerRepository: any,
    fileHistoryRepository: FileHistoryRepository
  ) {
    const uploadedFiles = request.files;
    const findbyemail = await customerRepository.find({
      fields: {login_history: false, file_history: false},
      where: { email: request.headers.email },
    });
    if (findbyemail.length <= 0 || !request.headers.email)
      return { result: "Email not found" };

    const mapper = (f: globalThis.Express.Multer.File) => ({
      fieldname: f.fieldname,
      originalname: f.originalname,
      encoding: f.encoding,
      mimetype: f.mimetype,
      size: f.size,
    });
    let files: object[] = [];
    if (Array.isArray(uploadedFiles)) {
      files = uploadedFiles.map(mapper);
    } else {
      for (const filename in uploadedFiles) {
        files.push(...uploadedFiles[filename].map(mapper));
      }
    }

    const fileArray = request.files as Express.Multer.File[];
    const originalname = fileArray[0].originalname;
    const fileByUser = await fileHistoryRepository.find({
      where: { and: [{email: findbyemail[0].email},{filename: originalname }] },
    })
    let isFileExist = fileByUser.some((file: any) => {
      return file.filename === originalname
    });
    if (isFileExist)
      throw new HttpErrors.Conflict("File name already exists, please select a different file name");

    let file = path.join(__dirname, `../../.sandbox/${originalname}`); // Path to and name of object. For example '../myFiles/index.js'.
    let fileStream = fs.createReadStream(file);
  
    const fileFormat: string = file.split(".").pop()!;
    if (
        fileFormat.includes("csv") ||
        fileFormat.includes("xlsx") ||
        fileFormat.includes("CSV") ||
        fileFormat.includes("XLSX")
    ) {
      if (findbyemail.length > 0) {
        let email_address = typeof(request.headers.email) == 'string'? request.headers.email : request.headers.email[0];
        if (email_address) {
          // Things left to do here: 
          // 1) Validate the file is correct
          // 2) Save the file name in file_history, 
          // 3) write a python script that triggers updating of the ML algorithm?
          // 
        
          return {
            success: true,
            message: "File has been uploaded successfully",
          };
        } 
      }
    } else {
      throw new HttpErrors.NotFound("File must be in csv or xlsx format");
    }

  }

  /**
   * Get files and fields for the request
   * @param request - Http request
   */
  private static async getNewFilesAndFields(
    request: Request,
    customerRepository: any,
    fileHistoryRepository: FileHistoryRepository,
    customerIndustryRepository: CustomerIndustryRepository,
    transactionHistoryRepository: TransactionHistoryRepository,
    adminEventsRepository: AdminEventsRepository,
    integrationsRepository: IntegrationsRepository,
    generateLeadsRepository:GenerateLeadsRepository,
    rowCount: number,
    external_order_id:string
  ) {
    const uploadedFiles = request.files;
    const findbyemail = await customerRepository.find({
      fields: {login_history: false, file_history: false},
      where: { email: request.headers.email },
    });
    if (findbyemail.length <= 0 || !request.headers.email)
      return { result: "Email not found" };
    const fileArray = request.files as Express.Multer.File[];
    const originalname = fileArray[0].originalname;

    let file = path.join(__dirname, `../../.sandbox/${originalname}`); // Path to and name of object. For example '../myFiles/index.js'.
        
    let findIndustryProfile = await customerIndustryRepository.findOne({ where: { email: request.headers.email } })
    
    // TODO: Return if customer's industry profile not found
    const industryProfile = findIndustryProfile?.industry_profile || []

    const mapper = (f: globalThis.Express.Multer.File) => ({
      fieldname: f.fieldname,
      originalname: f.originalname,
      encoding: f.encoding,
      mimetype: f.mimetype,
      size: f.size,
    });
    let files: object[] = [];
    if (Array.isArray(uploadedFiles)) {
      files = uploadedFiles.map(mapper);
    } else {
      for (const filename in uploadedFiles) {
        files.push(...uploadedFiles[filename].map(mapper));
      }
    }

    
    try {
      const fileByUser = await fileHistoryRepository.find({
        where: { and: [{ email: findbyemail[0].email }, { filename: originalname }] },
      })
      let isFileExist = fileByUser.some((file: any) => {
        return file.filename === originalname
      });
      if (isFileExist)
        throw new HttpErrors.Conflict("File name already exists, please select a different file name");

      let fileStream = fs.createReadStream(file);
      // const getPlan = findbyemail[0].pricing_plan.plan;

      const fileFormat: string = file.split(".").pop()!;
      if (
        fileFormat.includes("csv") ||
        fileFormat.includes("xlsx") ||
        fileFormat.includes("CSV") ||
        fileFormat.includes("XLSX")
      ) {
        

        if (findbyemail.length > 0) {
          const industrialProfile = industryProfile.filter((ele: { id: any; }) => ele.id == request.body.industry_profile_id)
        //   const investProfile = investmentProfile.filter((ele: { id: any; }) => ele.id == request.body.investment_profile_id)

          let email_address = typeof (request.headers.email) == 'string' ? request.headers.email : request.headers.email[0];
          let file_history_obj = new FileHistory({
            email: findbyemail[0].email,
            filename: originalname,
            upload_date: new Date(),
            record_count: rowCount,
            status: 2,
            file_extension: fileFormat,
            industry_profile: industrialProfile && industrialProfile.length ? industrialProfile[0].question_answers : {} as any,
            external_order_id
          });
          file_history_obj = await fileHistoryRepository.save(file_history_obj);
          await UploadS3(originalname, fileStream, request.headers.email as string)
          
          const customerData = findbyemail[0];

          FileUploadProvider.fileValidateService(customerRepository, fileHistoryRepository, transactionHistoryRepository, adminEventsRepository, customerIndustryRepository,
             integrationsRepository,generateLeadsRepository ,email_address, originalname,undefined,false);
        }
      } else {
        throw new HttpErrors.NotFound("File must be in csv or xlsx format");
      }
    } catch (e) {
      throw new HttpErrors.InternalServerError(e);
    } finally {
      if (fs.existsSync(file)) {
        fs.unlinkSync(file)
      }
    }
    return {
      success: true,
      message: "File has been uploaded successfully",
    };
  }

  @authenticate('jwt')
  @post('/file_validate', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
              // required: ['email', 'filename'],
              properties: {
                  email: {
                      type: 'string',
                  },
                  filename: {
                      type: 'string'
                  },
              }
            },
          },
        },
        description: 'Files and fields',
      },
    },
  })
  async fileValidate(
    @requestBody() fileHandler: object,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {

    let customerEmail = request.headers.email as string;
    let fileName = request.headers.filename as string;

    if(request?.body?.email){
      let adminData = await this.customerRepository.findOne({fields: ['email'],
        where: { role: 'admin', email: customerEmail },
      });
      if(adminData){
        customerEmail = request.body.email as string;
        fileName = request.body.filename as string;
      }else {
        // return responseToSend;
        return response.status(400).send({msg:'Invalid request', statusCode: 400, error: 'This feature is for Admin only'});
      }
    } 
    
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        const responseFromFunction =  FileUploadProvider.fileValidateService(this.customerRepository, this.fileHistoryRepository, this.transactionHistoryRepository, 
          this.adminEventsRepository, this.customerIndustryRepository, this.integrationsRepository,this.generateLeadsRepository, customerEmail, fileName, request.body.columnMapping);
        resolve(response.status(200).send({msg: "File Submitted Successfully"}));
      });
    });
  }

  @authenticate('jwt')
  @post('/file_preprocess', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Pre Process File ',
      },
    },
  })
  async filePreProcess(
    @requestBody() fileHandler: object,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async (err: unknown) => {
        if (err) reject(err);

        let customerEmail = request.headers.email as string;
        let fileName = request.headers.filename as string;

        if (request?.body?.email) {
          let adminData = await this.customerRepository.findOne({
            fields: ['email'],
            where: { role: 'admin', email: customerEmail },
          });
          if (adminData) {
            customerEmail = request.body.email as string;
            fileName = request.body.filename as string;
            console.log("filename is: ",fileName);
          } else {
            // return responseToSend;
            return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
          }
        } 



        // @ts-ignore
        resolve(FileUploadProvider.filePreProcessService(this.customerRepository, this.fileHistoryRepository, this.customerIndustryRepository, this.integrationsRepository,this.generateLeadsRepository, customerEmail, fileName))
      });
    });
  }

  @authenticate('jwt')
  @post('/file_bettyprocess', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Betty Process File ',
      },
    },
  })
  async fileBettyProcess(
      @requestBody() fileHandler: object,
      @inject(RestBindings.Http.REQUEST)
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        resolve(FileUploadProvider.bettyProcessingService(this.customerRepository, this.fileHistoryRepository, this.customerIndustryRepository, this.integrationsRepository,this.generateLeadsRepository, request.headers.email, request.headers.filename))
      });
    });
  }

  @authenticate('jwt')
  @post('/file_download', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Betty Process File ',
      },
    },
  })
  async downloadResultProcess(
      @requestBody() fileHandler: object,
      @inject(RestBindings.Http.REQUEST)
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, (err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        resolve(FileUploadProvider.downloadResultService(this.customerRepository, this.fileHistoryRepository, this.customerIndustryRepository, this.integrationsRepository,request.headers.email, request.headers.filename))
      });
    });
  }

  @authenticate('jwt')
  @get('/file_history',{
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Betty Process File ',
      },
    },
  })
  async getFileHistory(
      @inject(RestBindings.Http.REQUEST)
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object>{
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, (err: unknown) => {
        if (err) reject(err);
        resolve(FileUploadController.getFileHistoryService(request, this.customerRepository, this.fileHistoryRepository))
      });
    });
  }

  @authenticate('jwt')
  @get('/file_history_paginated',{
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Betty Process File ',
      },
    },
  })
  async getFileHistoryPaginated(
      @inject(RestBindings.Http.REQUEST)
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object>{
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, (err: unknown) => {
        if (err) reject(err);
        resolve(FileUploadProvider.getFileHistoryPaginatedService(request, this.customerRepository, this.fileHistoryRepository))
      });
    });
  }



  @authenticate("jwt")
  @post("/check_duplicate_file")
  @response(200, {
    description: "Check duplicate file in history",
    content: {
      "application/json": {
        schema: {
          type: "object",
        },
      },
    },
  })
  async checkDuplicateFile(
    @requestBody({
      content: {
        "application/json": {
          schema: {
            type: "object",
          },
        },
      },
    })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<Response<any, Record<string, any>>> {
    // initializing variable for response
    let msg = "File checked successfully";
    let responseToSend = { data: {}, msg };
    let status = 200;
    try {
        const getDuplicateFile = await FileUploadProvider.checkFileUploadedService(request, this.customerRepository, this.fileHistoryRepository)
        
        const {statusCode, data, msg} = getDuplicateFile;
        responseToSend.msg = msg;
        responseToSend.data = data || false;
        status = statusCode;

      return response.status(status).send(responseToSend);
    } catch(error) {
      console.error("error in find duplicate file", error);
      responseToSend.msg = error.message;
      return response.status(500).send(responseToSend);
    }
  }

  

  @authenticate('jwt')
  @get('/getFileContent', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
            },
          },
        },
        description: 'Betty Process File ',
      },
    },
  })
async getFileContent(
  @inject(RestBindings.Http.REQUEST)
  request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<object> {
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, (err: unknown) => {
        if (err) reject(err);
        resolve(FileUploadProvider.fileContentService(request, this.customerRepository, this.fileHistoryRepository));
      });
    });
  }

  /**
   * Get files and fields for the request
   * @param request - Http request
   */
  private static async getFileHistoryService(
    request: Request,
    customerRepository: any,
    fileHistoryRepository: FileHistoryRepository
    ) {
        const findbyemail = await customerRepository.find({
            fields: {login_history: false, file_history: false},
            where: { email: request.headers.email },
        });
        if (findbyemail.length <= 0 || !request.headers.email)
            return { result: "Email not found" };
        //dont show lead generation files
        let previous_files = await fileHistoryRepository.find({where: {email: findbyemail[0].email,source:{neq:usageType.LEADGENERATION}}, 
                                                                order: ['upload_date DESC']});

        return previous_files
    }



  @authenticate('jwt')
  @post('/postProcessingValidationService', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'object',
              // required: ['email', 'filename'],
              properties: {
                  email: {
                      type: 'string',
                  },
                  filename: {
                      type: 'string'
                  },
              }
            },
          },
        },
        description: 'Betty Process File ',
      },
    },
  })
  async postProcessingValidationService(
      @requestBody() fileHandler: object,
      @inject(RestBindings.Http.REQUEST)
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {

    let customerEmail = request.headers.email as string;
    let fileName = request.headers.filename as string;
    let isAdmin = false;
    if(request?.body?.email){
      let adminData = await this.customerRepository.findOne({fields: ['email'],
        where: { role: 'admin', email: customerEmail },
      });
      if(adminData){
        customerEmail = request.body.email as string;
        fileName = request.body.filename as string;
        isAdmin = true;
      }else {
        // return responseToSend;
        return response.status(400).send({msg:'Invalid request', statusCode: 400, error: 'This feature is for Admin only'});
      }
    } 
    return new Promise<object>((resolve, reject) => {
      this.handler(request, response, async(err: unknown) => {
        if (err) reject(err);
        // @ts-ignore
        let responseFromFunction = await FileUploadProvider.postProcessingValidationService(this.customerRepository, this.fileHistoryRepository, this.customerIndustryRepository, this.integrationsRepository,customerEmail, fileName, isAdmin);
        resolve(response.status(responseFromFunction.statusCode).send(responseFromFunction));
      });
    });
  }


  @authenticate('jwt')
  @post("/uploadFile", {
    responses: {
      200: {
        content: {
          "application/json": {
            schema: {
              type: "object",
              },
            },
          },
          description: "Files and fields",
        },
      },
  })
  async uploadAnyFile(
      @requestBody.file()
          request: Request,
      @inject(RestBindings.Http.RESPONSE) response: Response
  ): Promise<object> {
    let statusCode = 400;

    let responseObject = {
      success: false,
      message: "file uploded successfully",
      data: null
    }
    const emailFromHeaders = request.headers.email;
    const fileName = request.headers.filename;
    // const customerEmail = request.headers.uploademail
    // const emailFromBody = customerEmail as string;
   
    const filePath = path.join(__dirname, `../../.sandbox/${fileName}`)

    try {
      if ((typeof fileName === "string") && (typeof emailFromHeaders === "string")) {
        let fileNames = fileName.split(".");
        const fileFormat: string = fileNames[fileNames.length - 1].toLowerCase();
        if (fileFormat == "csv") {
          let adminsData = await this.customerRepository.findOne({
            where: { email: emailFromHeaders, role: "admin" },
          });
          if (adminsData) {
            responseObject.success = true;
            statusCode = 200;
            return new Promise<object>((resolve, reject) => {
              this.handler(request, response, async (err: unknown) => {
                if (err) reject(err);
                else {
                  try {
                    const customerEmail = request?.body?.email as string;
                    const toReadStream = fs.createReadStream(filePath)
                    await UploadS3(fileName, toReadStream, customerEmail)
                  } catch (e) {
                    responseObject.message = e.message;
                  }
                  finally {
                    if (fs.existsSync(filePath)) fs.unlinkSync(filePath)
                  }
                  return response.status(statusCode).send(responseObject)
                }
              });
            });
          } else {
            responseObject.message = "Only Admin can use this feature";
          }
        } else {
          responseObject.message = "Only CSV file is supported";
        }
      } else {
        responseObject.message = "filename or email is mising";
      }
    } catch (e) {
      responseObject.message = e.message;
    }

    return response.status(statusCode).send(responseObject);
  }



}
export class FileHandler{
  filename: string;
  columnMapping: object;
}
