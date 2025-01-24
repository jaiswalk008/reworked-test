import { inject } from '@loopback/core';
import {
  get,
  HttpErrors,
  oas,
  param,
  Response,
  Request,
  requestBody,
  RestBindings,
} from '@loopback/rest';
import fs from 'fs';
import path from 'path';
import { promisify } from 'util';
import { STORAGE_DIRECTORY } from '../keys';
import { TokenService, authenticate } from '@loopback/authentication';
import { FileHistoryRepository } from '../repositories';
import { TokenServiceBindings, UserRepository } from '@loopback/authentication-jwt';
import {
  repository,
} from '@loopback/repository';
import * as jwt from 'jwt-simple';
import { downloadFileFromS3, checkS3ObjectExistence } from '../services';
import { checkforCustomBranding } from '../helper';

const readdir = promisify(fs.readdir);

/**
 * A controller to handle file downloads using multipart/form-data media type
 */

export class FileDownloadController {
  constructor(@inject(TokenServiceBindings.TOKEN_SERVICE)
  public jwtService: TokenService,
    @inject(STORAGE_DIRECTORY) private storageDirectory: string,
    @repository(FileHistoryRepository)
    protected fileHistoryRepository: FileHistoryRepository) { }
  @authenticate('jwt')
  @get('/files', {
    responses: {
      200: {
        content: {
          'application/json': {
            schema: {
              type: 'array',
              items: {
                type: 'string',
              },
            },
          },
        },
        description: 'A list of files',
      },
    },
  })
  async listFiles() {
    const files = await readdir(this.storageDirectory);
    return files;
  }

  // @authenticate('jwt')
  // @get('/files/')
  // @oas.response.file()
  // async downloadFile(
  //   @param.query.string('filename') fileName: string,
  //   @param.query.string('token') token: string,
  //   @inject(RestBindings.Http.RESPONSE) response: Response,
  // ) {
  //   const secret = process.env.secretKey || '';
  //   const decodedToken = jwt.decode(token, secret);
  //   const file = await this.validateFileNameAndToken(fileName, decodedToken);
  //   response.download(file, fileName);
  //   return response;
  // }

  @authenticate('jwt')
  @get('/files/{filename}')
  @oas.response.file()
  async downloadUserlFile(
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @param.path.string('filename') fileName: string,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {
    let responseData = { error: "", downloadUrl: "" }
    try {
      
      let emailUsedToFetchData = request.headers.email as string
      const s3Url = await this.generateS3Url(fileName, emailUsedToFetchData);
      responseData.downloadUrl = s3Url;
      response.send(responseData)
    } catch (error) {
      responseData.error = error.message
    }
    return response
  }

  @authenticate('jwt')
  @get('/files/{filename}/{customerEmail}')
  @oas.response.file()
  async downloadMailFile(
    @inject(RestBindings.Http.REQUEST)
    request: Request,
    @param.path.string('filename') fileName: string,
    @param.path.string('customerEmail') customerEmail: string,
    @inject(RestBindings.Http.RESPONSE) response: Response,
  ) {

    let responseData = { error: "", downloadUrl: "" }
    try {
      let emailUsedToFetchData = request.headers.email as string
      if (customerEmail) {
        emailUsedToFetchData = customerEmail
      }

      const s3Url = await this.generateS3Url(fileName, emailUsedToFetchData);
      responseData.downloadUrl = s3Url;
      response.send(responseData)

    } catch (error) {
      responseData.error = error.message
    }
    return response
  }
  /**
   * Validate file names to prevent them goes beyond the designated directory
   * @param fileName - File name
   */
  private validateFileName(fileName: string) {
    const resolved = path.resolve(this.storageDirectory, fileName);
    if (resolved.startsWith(this.storageDirectory)) return resolved;
    // The resolved file is outside sandbox
    throw new HttpErrors.BadRequest(`Invalid file name: ${fileName}`);
  }

  private async validateFileNameAndToken(fileName: string, token: any) {
    let fileNameToSearch = fileName;
    if (fileName.startsWith("BETTY_")) {
      fileNameToSearch = fileName.replace("BETTY_", "");
    }
    let filehistoryData = await this.fileHistoryRepository.find({
      fields: ['email'],
      where: { email: token.email, filename: fileNameToSearch },
    });
    if (!(filehistoryData && filehistoryData.length)) throw new HttpErrors.BadRequest(`Invalid Token`);
    const resolved = path.resolve(this.storageDirectory, fileName);
    if (resolved.startsWith(this.storageDirectory)) return resolved;
    // The resolved file is outside sandbox
    throw new HttpErrors.BadRequest(`Invalid file name: ${fileName}`);
  }

  private async generateS3Url(fileName: string, email: string) {
    try {
      const emailUsedToFetchData = email;
      let file = this.validateFileName(fileName);
      const s3Key = emailUsedToFetchData + '/' + fileName;

      let s3ObjectUrl = await downloadFileFromS3(path.basename(file), emailUsedToFetchData)

      const ifUrlValid = await checkS3ObjectExistence(s3Key);

      // fallback url. if not valid url then check with whitelabling file name
      if (!ifUrlValid && fileName.startsWith("BETTY_") && emailUsedToFetchData) {
        const brandPrefix = checkforCustomBranding(emailUsedToFetchData);
        fileName = fileName.replace("BETTY", brandPrefix);
        file = this.validateFileName(fileName);
        s3ObjectUrl = await downloadFileFromS3(path.basename(file), emailUsedToFetchData)
      }
      return s3ObjectUrl;

    } catch (error: any) {
      console.error("Error while generating url", error);
      return ""
    }
  }

}
