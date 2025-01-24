// Uncomment these imports to begin using these cool features!
import { inject } from "@loopback/core";
import {
  Filter,
} from '@loopback/repository';
import {
  param,
  get,
  getModelSchemaRef,
  response,
  del,
  Request,
  Response,
  RestBindings,
  oas,
  post,
  requestBody,
} from '@loopback/rest';
import { CustomerRepository, FileHistoryRepository } from "../repositories";

import { repository } from "@loopback/repository";
import { FileHistory } from "../models";
import { TokenService, authenticate } from '@loopback/authentication';
import { TokenServiceBindings, UserRepository } from '@loopback/authentication-jwt';
import { FileHistoryService } from "../services";
import { unlinkSync } from "fs";
import { getFilterSort } from "../helper/filter-sort";

/**
 * A controller to handle file uploads using multipart/form-data media type
 */
type FileHistoryResponse = {
  data: FileHistory[];
  length: number;
};
export class FileHistoryController {
  /**
   * Constructor
   * @param fileHistoryRepository
   * @param customerRepository
   */
  constructor(
    @inject(TokenServiceBindings.TOKEN_SERVICE)
    public jwtService: TokenService,
    @repository(FileHistoryRepository)
    public fileHistoryRepository: FileHistoryRepository,
    @repository(CustomerRepository)
    public customerRepository: CustomerRepository,

  ) { }

  @authenticate('jwt')
  @get('/get_usage_stats_for_month/{yearNumber}/{monthNumber}')
  @oas.response.file()
  @response(200, {
    description: 'Gets list of users and their plans whove not used the product for particular month',
    content: {
      'application/json': {
        schema: {
          //type: 'array',
          //items: getModelSchemaRef(FileHistory, {includeRelations: true}),
        },
      },
    },
  })
  async get_usage_stats_for_month(
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @param.path.integer('monthNumber') monthNumber: number,
    @param.path.integer('yearNumber') yearNumber: number,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {
    let adminEmail = request.headers.email as string;
    if (!adminEmail) adminEmail = ''
    const adminData = await this.customerRepository.findOne({
      fields: ['email'],
      where: { role: 'admin', email: adminEmail },
    });
    if (!adminData) return response.status(403).send('Request should be made by admin')
    const filePath = await FileHistoryService.getUsageStatsForMonth(monthNumber, yearNumber, this.customerRepository, this.fileHistoryRepository);
    if (filePath == '') return response.send('Not Found')
    return await new Promise((resolve: any, reject: any) => {
      response.download(filePath, (err: any) => {
        if (err) reject(err);
        unlinkSync(filePath)
        resolve();
      });
    });
  }
  //  @authenticate('jwt')
  //  @get('/get_all_file_history')
  //  @response(200, {
  //    description: 'Array of FileHistory model instances',
  //    content: {
  //      'application/json': {
  //        schema: {
  //          type: 'array',
  //          items: getModelSchemaRef(FileHistory, { includeRelations: true }),
  //        },
  //      },
  //    },
  //  })
  //  async find(
  //    @param.filter(FileHistory) filter?: Filter<FileHistory>,
  //  ): Promise<FileHistory[]> {
  //    return this.fileHistoryRepository.find({ order: ['upload_date DESC'] }, filter);
  //  }

  @authenticate('jwt')
  @post('/file_status_change')
  @response(200, {
    description: 'File status change',
    content: {
        'application/json': {
          schema: {
            type: 'object',
          },
        },
      },
  })
  async fileStatusChange(
    @requestBody({
        content: {
          "application/json": {
            schema: {
              type: "object",
                  required: ["fileId", "status"],
              properties: {
                fileId: {
                    type: 'string',
                },
                status: {
                    type: 'number'
                },
              },
            },
          },
        },
      })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {

    let msg = "File status changed successfully"
    console.log(request)
    try {
        const fileId = request.body.fileId as string;
        const status = request.body.status as number;
        let idAdmin = false;
        let adminEmail = request.headers.email as string;
        if (adminEmail) {
          idAdmin = true;
          if (!adminEmail) adminEmail = ''
          const adminData = await this.customerRepository.findOne({
            fields: ['email'],
            where: { role: 'admin', email: adminEmail },
          });
          if (!adminData) return res.status(403).send({ msg: 'Request should be made by admin' })
          let fileData = await this.fileHistoryRepository.findOne({ where: { id: fileId } });
          if(fileData?.status) {
            fileData.status = status
            await this.fileHistoryRepository.update(fileData)
            return res.status(200).send({ msg });
        } else  return res.status(404).send({ msg: 'Status not found in file.' });
        } else {
            return res.status(404).send({ msg: 'Email not found in headers.' });
        }
    }  catch (error) {
        console.log(error);
        return res.status(500).send({ msg: error.message });
    }
  }

  @authenticate('jwt')
  @get('/get_all_file_history')
  @response(200, {
    description: 'Array of FileHistory model instances',
    content: {
      'application/json': {
        schema: {
          type: 'array',
          items: getModelSchemaRef(FileHistory, { includeRelations: true }),
        },
      },
    },
  })
  async find(
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @param.filter(FileHistory) filter?: Filter<FileHistory>,
  ): Promise<FileHistoryResponse> {

    let where: any = {}
    let order: string = 'upload_date DESC'

    const email: any = request.query.email;
    const sortName: any = request.query.sortName;
    const type: any = request.query.type;
    const page: any = request.query.page || 1;

    if (email) {
      where['email'] = email
    }

    const filtertype = "v1"
    const filterSort = getFilterSort({where, filtertype, sortName, type})
    
    where = filterSort.where
    if (filterSort.order !== '') {
        order = filterSort.order
    }

    const limit = 10;
    const offset = (page - 1) * limit;

    const totalCountPromise = this.fileHistoryRepository.count(
        where
    );

    const dataPromise = this.fileHistoryRepository.find({
      where: where,
      order: [order],
      limit: limit, // This is the 'limit'
      skip: offset, // This is the 'offset'
    });
    const [totalCount, data] = await Promise.all([totalCountPromise, dataPromise]);
    const length = totalCount?.count

    // const pageData = data.splice( (page-1) * 10, 10)
    return {
      length: length,
      data: data
    };
  }

  @authenticate('jwt')
  @del('/delete_file/{id}')
  @response(204, {
    description: 'File DELETE success',
  })
  async delete_file(
    @param.path.string('id') id: string,
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<any> {
    let adminEmail = request.headers.email as string;
    let adminData = await this.customerRepository.findOne({
      fields: ['email'],
      where: { role: 'admin', email: adminEmail },
    });

    if (adminData) {
      await this.fileHistoryRepository.deleteById(id);
    } else {
      return response.status(400).send({ msg: 'Invalid request', statusCode: 400, error: 'This feature is for Admin only' });
    }
  }

  @authenticate('jwt')
  @get('/file_process_reports')
  @response(200, {
    description: 'Array of FileHistory model instances',
    content: {
      'application/json': {
          schema: {
            type: 'object',
          },
        },
    },
  })
  async fileProcessReports(
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response,
    @param.filter(FileHistory) filter?: Filter<FileHistory>,
  ): Promise<any> {

    try {
      const authHeader = request.headers.authorization;

      const responseFromFunction = await FileHistoryService.getParentAccountReport(request,this.customerRepository,this.fileHistoryRepository, authHeader);

      if (responseFromFunction) {
        const { status, filePath, exportCSV, ...responseWithoutStatus } = responseFromFunction;
        return res.status(status).send(responseWithoutStatus);
      } else {
        return res.status(500).send({ msg: 'Failed to generate report', data: {} });
      }
    } catch (error: any) {
      console.log('Error in reports API:', error);

      return res.status(500).send({
        msg: error.message || 'Internal server error',data: {},
      });
    }
    
  }

    @authenticate('jwt')
    @get('/file_process_reports_csv')
    @response(200, {
      description: 'Array of FileHistory model instances',
      content: {
        'application/json': {
            schema: {
              type: 'object',
            },
          },
      },
    })
    async fileProcessReportsCSV(
      @inject(RestBindings.Http.REQUEST)
      request: Request,
      @inject(RestBindings.Http.RESPONSE) res: Response,
      @param.filter(FileHistory) filter?: Filter<FileHistory>,
    ): Promise<any> {
  
      try {
        const authHeader = request.headers.authorization;
        const exportCSV = true;
        const responseFromFunction = await FileHistoryService.getParentAccountReport(request, this.customerRepository, this.fileHistoryRepository, authHeader, exportCSV)
        if (!responseFromFunction?.filePath) return res.status(400).send({ msg: 'File Not Found', data: {} });
          return await new Promise((resolve: any, reject: any) => {
            res.download(responseFromFunction.filePath, (err: any) => {
              if (err) reject(err);
              unlinkSync(responseFromFunction.filePath)
              resolve();
            });
          });
      }
      catch(error){
        console.log("Error in reports csv api", error.message);
        return res.status(500).send({ msg: error.message, data: {} });
      }
      
    }

    @authenticate('jwt')
    @get('/report_all_emails')
    @response(200, {
      description: 'Array of FileHistory model instances',
      content: {
        'application/json': {
            schema: {
              type: 'object',
            },
          },
      },
    })
    async getAllEmails(
      @inject(RestBindings.Http.REQUEST)
      request: Request,
      @inject(RestBindings.Http.RESPONSE) res: Response,
      @param.filter(FileHistory) filter?: Filter<FileHistory>,
    ): Promise<any> {
  
      try {
        const email: string = request.query.email as string;
        if(!email)
          return res.status(400).send({ msg: 'Email is required', data: {} });
        const where: any = {};
      
        // Fetch customer details to determine if the user is an admin
        const customer = await this.customerRepository.findOne({
          fields: { email: true, role: true },
          where: { email }
        });
      
        // If the customer is not an admin, apply filter for parent_email
        if (customer?.role !== 'admin') {
          where['parent_email'] = email;
        }
      
        // Fetch customers based on the `where` clause
        const allCustomers = await this.customerRepository.find({
          fields: { email: true, parent_email: true },
          where,
        });
      
        const parentChildMap: { [key: string]: string[] } = {};
    
        allCustomers.forEach((customer) => {
          const parentEmail = customer.parent_email;
          const childEmail = customer.email;
    
          if (parentEmail) {
            if (!parentChildMap[parentEmail]) {
              parentChildMap[parentEmail] = [];
            }
            parentChildMap[parentEmail].push(childEmail);
          }
        });
    
      
      
        const allParentEmails = [...new Set(
          allCustomers
            .map((ele: any) => ele.parent_email)
            .filter((email: string | null) => email)
        )].sort();
      
        const allChildEmails = [...new Set(
          allCustomers
            .map((ele: any) => ele.email)
            .filter((email: string | null) => email)
        )].sort();
      
        return res.status(200).send({
          msg: "Data fetched successfully",
          data: { allChildEmails, allParentEmails,parentChildMap },
        });
      } catch (error) {
        console.error('Error in getAllEmails API', error.message);
        return res.status(500).send({ msg: error.message, data: {} });
      }
      
      
    }
    
  @authenticate('jwt')
  @post('/update-filehistory')
  @response(200, {
    description: 'File History update ',
    content: {
        'application/json': {
          schema: {
            type: 'object',
          },
        },
      },
  })
  async updateFilstory(
    @requestBody({
        content: {
          "application/json": {
            schema: {
              type: "object",
              required: ["fileId", "externalOrderId"],
              properties: {
                fileId: {
                    type: 'string',
                },
                externalOrderId: {
                    type: 'string'
                },
              },
            },
          },
        },
      })
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @inject(RestBindings.Http.RESPONSE) res: Response
  ): Promise<Response<any>> {

    let msg = "File history updated successfully"
    try {
        const fileId = request.body.fileId as string;
        const externalOrderId = request.body.externalOrderId as string;
        let fileData = await this.fileHistoryRepository.findOne({ where: { id: fileId } });
        if(fileData){
          fileData.external_order_id = externalOrderId
          await this.fileHistoryRepository.update(fileData)
          return res.status(200).send({ msg });
        } else  return res.status(404).send({ msg: 'Invalid file id.' });
    }  catch (error) {
        console.log('Error in update-filstory', error);
        return res.status(500).send({ msg: error.message });
    }
  }

  
  // @get('/convert-status')
  // @response(200, {
  //   description: 'Convert string Status to integer',
  //   content: {
  //     'application/json': {
  //       schema: {
  //       },
  //     },
  //   },
  // })
  // async convertStatus(): Promise<any> {

  //   const documents = await this.fileHistoryRepository.find(
  //     {
  //       where: {
  //         status: { "nin": [1, 2, 3, 4, 5, 6, 7] }
  //       }
  //     });
  //     const promsies = [];
  //   for (const doc of documents) {
  //     if (typeof doc.status == 'string') {
  //       const statuss = parseInt(doc.status);
  //       promsies.push(this.fileHistoryRepository.updateById(doc.id, { status: parseInt(doc.status) }));
  //     }
  //   }
  //   await Promise.all(promsies);
  //   console.log("Done")
  //   return {
  //     msg: `Total ${promsies.length} statuses converted successfully`
  //   };
  // }
}
