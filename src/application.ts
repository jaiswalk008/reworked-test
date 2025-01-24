import {BootMixin} from '@loopback/boot';
import {ApplicationConfig, createBindingFromClass} from '@loopback/core';
import {
  RestExplorerBindings,
  RestExplorerComponent,
} from '@loopback/rest-explorer';
import {RepositoryMixin} from '@loopback/repository';
import {RestApplication} from '@loopback/rest';
import {ServiceMixin} from '@loopback/service-proxy';
import path from 'path';
import {MySequence} from './sequence';
import multer from 'multer'; 
import{FILE_UPLOAD_SERVICE,STORAGE_DIRECTORY} from './keys'; 
import { AuthenticationComponent } from '@loopback/authentication';
import {
  JWTAuthenticationComponent,
  SECURITY_SCHEME_SPEC,
  UserServiceBindings,
} from '@loopback/authentication-jwt';
import { DbDataSource } from './datasources';
import {CronComponent} from '@loopback/cron';
import { RollOverCronJob } from './cronJob';
import { ErrorHandlerSequence } from './error_handler/error_handler_sequence'; // Import your new sequence


export {ApplicationConfig};

export class ProcessControlApplication extends BootMixin(
  ServiceMixin(RepositoryMixin(RestApplication)),
) {
  constructor(options: ApplicationConfig = {}) {
    super(options);

    // Set up the custom sequence
    this.sequence(MySequence);
    
    // Bind the custom sequence
    // this.sequence(ErrorHandlerSequence);

    // Set up default home page
    this.static('/', path.join(__dirname, '../public'));

    // Customize @loopback/rest-explorer configuration here
    this.configure(RestExplorerBindings.COMPONENT).to({
      path: '/explorer',
    });
    this.component(RestExplorerComponent);
    this.configureFileUpload(options.fileStorageDirectory);

    this.projectRoot = __dirname;
    // Customize @loopback/boot Booter Conventions here
    this.bootOptions = {
      controllers: {
        // Customize ControllerBooter Conventions here
        dirs: ['controllers'],
        extensions: ['.controller.js'],
        nested: true,
      },
    };
    this.component(AuthenticationComponent);
    // Mount jwt component
    this.component(JWTAuthenticationComponent);
    // Bind datasource
    this.dataSource(DbDataSource, UserServiceBindings.DATASOURCE_NAME);
    // Bind cron component
    this.component(CronComponent);
    this.add(createBindingFromClass(RollOverCronJob));
  }
 protected configureFileUpload(destination?: string) {
   // Upload files to `dist/.sandbox` by default
   destination = destination ?? path.join(__dirname, '../.sandbox');
   this.bind(STORAGE_DIRECTORY).to(destination);
   const multerOptions: multer.Options = {
     storage: multer.diskStorage({
       destination,
       // Use the original file name as is
       filename: (req, file, cb) => {
         cb(null, file.originalname);
       },
     }),
   };
   // Configure the file upload service with multer options
   this.configure(FILE_UPLOAD_SERVICE).to(multerOptions);
  }


}
