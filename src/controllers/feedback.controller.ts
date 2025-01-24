import {inject} from '@loopback/core';
import { repository} from '@loopback/repository';
import { post, requestBody, HttpErrors,Response,RestBindings,Request,get,patch,del } from '@loopback/rest';
import { FILE_UPLOAD_SERVICE } from '../keys';
import { FileUploadHandler } from '../types';
import { FeedbackRepository } from '../repositories/feedback.repository';
import { authenticate } from '@loopback/authentication';
import { generatePresignedS3Url, UploadS3 } from '../services';
import fs from 'fs';
import { extractDetailsFromAuthToken } from '../helper/utils';
import { Feedback } from '../models/feedback.model';
import { CustomerRepository } from '../repositories';
import { sendEmailToAdmin, sendMailChimpEmail } from '../helper';
interface File {
  originalname: string;
  encoding: string;
  mimetype: string;
  buffer: Buffer;
  path: string;
}
export class FeedbackController {

  constructor(
    @repository(FeedbackRepository)
    public feedbackRepository: FeedbackRepository,
    @repository(CustomerRepository)
    public customerRepository: CustomerRepository,
    @inject(FILE_UPLOAD_SERVICE)
    private handler: FileUploadHandler,
  ) {
 
  }
 
  @authenticate('jwt')
  @post('/feedback', {
    responses: {
      200: {
        description: 'Feedback submission response',
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                success: { type: 'boolean' },
                message: { type: 'string' },
 
              },
            },
          },
        },
      },
    },
  })
  async submitFeedback(
    @requestBody.file() request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<object> {
    
    return new Promise((resolve, reject) => {
      
      this.handler(request, response, async (err: unknown) => {

        if (err) {
          console.error('File handler error:', err);
          reject(new HttpErrors.BadRequest('File upload failed.'));
          return;
        }

         const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
         const customer = await this.customerRepository.findOne({where:{email}})

        if (!email || !customer) {
          console.error("Unauthorized user");
          reject(new HttpErrors.BadRequest('Authorization error'));
          return;
        }
        const {category, title, feedback} = request.body;
        
        const files = request.files as { [fieldname: string]: File[]; };
        const file = Array.isArray(files) ? files[0] : files;
        
        let fileUrl = '';

        let feedbackEntry;  
        try {
          if (file) {
                        
            // Upload to S3
            const fileStream = fs.createReadStream(file.path);
            
            await UploadS3(file.originalname, fileStream, email);
            
            fileUrl = generatePresignedS3Url(file.originalname, email,undefined);
            if (fs.existsSync(file.path)) {
              fs.unlinkSync(file.path);
            }
          }  
          feedbackEntry = await this.feedbackRepository.create({
            category,
            title,
            feedback,
            email,
            file_url: fileUrl,
            status:"open",
            created_at: new Date(),
            updated_at: new Date(),
            
          }) as Feedback;
          const options:any  = { title,category,feedback};

          sendMailChimpEmail("feedback_template",email,'',customer.name,false,options);
          const optionsForAdminMail = {
            content: `
              User ${email} has submitted feedback. 
              Feedback Details:
              Title - ${title}
              Category - ${category}
              Feedback - ${feedback}
              
              Please review the submitted feedback
            `
          };
          
          // Sending the email
          sendEmailToAdmin('', {customer}, this.customerRepository, optionsForAdminMail);
          
          resolve({
            success: true,
           });
        } catch (error) {
          console.error('Error while processing feedback:', error);
          reject(
            new HttpErrors.InternalServerError(
              'An error occurred while processing feedback',
            ),
          );
        }
        return response.send({
          data:{id:feedbackEntry?.id,file_url:fileUrl},
          msg: "Feedback submitted successfully",
        })
      });
    });
  }

  @authenticate('jwt')
  @get('/feedback', {
    responses: {
      200: {
        description: 'List of Feedbacks',
        content: {
          'application/json': {
            schema: {
              type: 'array',
              items: {
                type: 'object',
                properties: {
                  category: { type: 'string' },
                  title: { type: 'string' },
                  feedback: { type: 'string' },
                  status: { type: 'string' },
                  updated_at:{type:'date'}
                },
              },
            },
          },
        },
      },
    },
  })
  async getFeedbacks(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response<Feedback[]>> {
    try {
       const filter: any = {
        where: {},
      };
      const inAdminPage = request.query['inAdminPage'] as string; 
      
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
      if (email && !inAdminPage) {
        filter.where.email = email;
      }
      const feedbacks = await this.feedbackRepository.find({
        ...filter,
        fields: {
          created_at: false,
        },
      });

      return response.status(200).send({data:feedbacks,msg:"Feedbacks fetched successfully"});
    } catch (error) {
      console.error('Error fetching feedbacks:', error);
      throw new HttpErrors.InternalServerError('Failed to fetch feedbacks');
    }
  }
  @authenticate('jwt')
  @patch('/feedback', {
    responses: {
      200: {
        description: 'Resolve a feedback',
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                msg: { type: 'string' },
              },
            },
          },
        },
      },
    },
  })
  async resolveFeedback(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response> {
    try {
      const feedbackId = request.query['feedback_id'] as string; 
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
      if (!feedbackId) {
        throw new HttpErrors.BadRequest('Feedback ID is required');
      }

      const customer = await this.customerRepository.findOne({where:{email}});
      if(customer?.role!=='admin'){
        throw new HttpErrors.NotFound('You need to be an admin to perform this operation!');
      }
      const feedback = await this.feedbackRepository.findById(feedbackId);

      if (!feedback) {
        throw new HttpErrors.NotFound('Feedback not found');
      }

      feedback.status = 'resolved';
      feedback.updated_at=new Date();
      await this.feedbackRepository.updateById(feedbackId, feedback);

      return response.status(200).send({
        msg: 'Feedback resolved successfully',
        data:{updatedAt:feedback.updated_at}
      });
    } catch (error) {
      console.error('Error resolving feedback:', error);
      throw new HttpErrors.InternalServerError('Failed to resolve feedback');
    }
  }

  @authenticate('jwt')
  @del('/feedback', {
    responses: {
      200: {
        description: 'Delete feedback',
        content: {
          'application/json': {
            schema: {
              type: 'object',
              properties: {
                msg: { type: 'string' },
              },
            },
          },
        },
      },
    },
  })
  async deleteFeedback(
    @inject(RestBindings.Http.REQUEST) request: Request,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ): Promise<Response> {
    try {
      const feedbackId = request.query['feedback_id'] as string;
      const { email } = extractDetailsFromAuthToken(request.headers.authorization || '');
  
      if (!feedbackId) {
        throw new HttpErrors.BadRequest('Feedback ID is required');
      }
      const feedback = await this.feedbackRepository.findById(feedbackId);
      if (!feedback) {
        throw new HttpErrors.NotFound('Feedback not found');
      }
  
      await this.feedbackRepository.deleteById(feedbackId);
  
      return response.status(200).send({
        msg: 'Feedback deleted successfully',
      });
    } catch (error) {
      console.error('Error deleting feedback:', error);
      if (error instanceof HttpErrors.HttpError) {
        throw error; 
      }
      throw new HttpErrors.InternalServerError('Failed to delete feedback');
    }
  }
  
}
