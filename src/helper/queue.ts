const Queue = require('bull');
import { pollCSVAvailability } from '../helper'
import { runPythonScript } from '../services';
import { redisClient, redisPublisher } from './redis';
// import { Queue } from 'bull'; // Import the Bull Queue
export const queue = new Queue('csv-polling', {
    redis: {
      host: 'localhost', // Redis server host
      port: 6379,        // Redis server port
      maxRetriesPerRequest: 20,
    }
});
export const columnMappingQueue = new Queue('column-mapping',{
  redis:{
    host:'caching-17f435c1-jaiswalk008-project1.h.aivencloud.com',
    port:24056,
    User:"default",
    password:"AVNS_HVGyea8oU5rrDI2oO16"
  }
})
columnMappingQueue.process(async (job:any) =>{
  try {
    console.log(`Processing job ${job.id}`);

  let python_output: any = await runPythonScript(job.data.scriptPath, job.data.args);
  console.log('in queue')
  // console.log(python_output)
    // await redisClient.hset(job.data.hashName, {fileid:job.data.fileId,taskCompleted:false,inQueue:false} );
    // console.log(`updated the value of hset ${job.data.hashName} of inqueue to false`)
    // console.log(`Job ${job.id} completed successfully`);
    // const message = JSON.stringify({ hashName:job.data.hashName, fileId:job.data.fileId, inQueue: false, taskCompleted: false });
    // await redisPublisher.publish('columnMappingTaskStatus', message);

    // return python_output;
    // call a function named map_columns and get the response 
  } catch (error) {
    console.error(`Error processing job ${job.id}:`, error);
    // You can handle errors, log them, or retry the job if needed
    throw error;
  }
})

// Process jobs from the queue
queue.process(async (job: any) => {
  try {
    console.log(`Processing job ${job.id}`);

    // Call the existing function to handle the main CSV polling logic
    await pollCSVAvailability(job);

    console.log(`Job ${job.id} completed successfully`);
  } catch (error) {
    console.error(`Error processing job ${job.id}:`, error);
    // You can handle errors, log them, or retry the job if needed
    throw error;
  }
});
