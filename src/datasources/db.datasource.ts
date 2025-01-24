import {inject, lifeCycleObserver, LifeCycleObserver} from '@loopback/core';
import {juggler} from '@loopback/repository';

// import dotenv from 'dotenv';

// dotenv.config()
// 
const config = {
  name: 'db',
  connector: 'mongodb',
  // url: 'mongodb+srv://reworkdev:PLKxXnSj4BduRWBl@dev.nyoqs.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
  // url: 'mongodb+srv://reworkprod:iA6cT7*ECppS@prod.4sh4q3t.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
  url: process.env.MONGO_URL,
   // url: 'mongodb+srv://reworkprod:iA6cT7*ECppS@prod.4sh4q3t.mongodb.net/myFirstDatabase?retryWrites=true&w=majority',
  //url: 'mongodb://localhost:27017/myFirstDatabase?readPreference=primary&directConnection=true&ssl=false',
  // host: 'localhost',
  // port: 27017,
  // user: 'reworkdev',
  // password: 'PLKxXnSj4BduRWBl',
  database: 'myFirstDatabase',
  authSource: "admin",
  useNewUrlParser: true,
  useUnifiedTopology: true
};

// Observe application's life cycle to disconnect the datasource when
// application is stopped. This allows the application to be shut down
// gracefully. The `stop()` method is inherited from `juggler.DataSource`.
// Learn more at https://loopback.io/doc/en/lb4/Life-cycle.html
@lifeCycleObserver('datasource')
export class DbDataSource extends juggler.DataSource
  implements LifeCycleObserver {
  static dataSourceName = 'db';
  static readonly defaultConfig = config;

  constructor(
    @inject('datasources.config.db', {optional: true})
    dsConfig: object = config,
  ) {
    super(dsConfig);
    this.setupIndexes();

  }
  async setupIndexes() {
    // Ensure the datasource is connected
    await this.connect();

    // Access the MongoDB native driver
    const db = this.connector?.client?.db();
    if (db) {
      // Helper function to check if an index exists
      const indexExists = async (collectionName: string, indexName: string) => {
        const indexes = await db.collection(collectionName).indexes();
        return indexes.some((index:{name:string}) => index.name === indexName);
      };

      // Define indexes
      const indexes = [
        { collection: 'DemographicData', field: 'uploaded_at', expireAfterSeconds: 604800 },//1 week
        { collection: 'BusinessData', field: 'uploaded_at', expireAfterSeconds: 2592000 },// 1 month
        { collection: 'PropertyData', field: 'uploaded_at', expireAfterSeconds: 2592000 } //1 month
      ];

      for (const { collection, field, expireAfterSeconds } of indexes) {
        const indexName = `${field}_1`; // Generate the index name based on field

        if (!await indexExists(collection, indexName)) {
          await db.collection(collection).createIndex(
            { [field]: 1 },
            { expireAfterSeconds }
          );
        } 
      }
    } else {
      console.error('Failed to access the MongoDB database');
    }
  }


}
