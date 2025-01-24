import asyncio
import signal
from bullmq import Queue, Worker
import logging
import json
import aioredis
from column_mapping_new import map_columns_new
import ast
# Redis connection details
REDIS_HOST = "caching-17f435c1-jaiswalk008-project1.h.aivencloud.com"
REDIS_PORT = 24056
REDIS_PASSWORD = "AVNS_HVGyea8oU5rrDI2oO16"
REDIS_USER = "default"

# Set up logging
logging.basicConfig(level=logging.INFO)

# Create the BullMQ queue
queue_name = "column-mapping"
   # Create Redis client
redis_url = f"redis://{REDIS_USER}:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}"
redis_client = aioredis.from_url(redis_url, decode_responses=True)
logging.info(redis_client)
logging.info(redis_url)
# Define the job processing function
async def process(job,job_token):
    hash_name = job.data.get('hashName')
    new_name_dict={}
    try:
        # Extract job data
        logging.info({'processing job with id= ':job.id})
        file_name = job.data.get("file_name")
        email = job.data.get("email")
        industry_profile = job.data.get("industry_profile")
        industry_profile_data = json.loads(industry_profile)
        custom_mapping = job.data.get("custom_mapping")
        logging.info({"custom":custom_mapping})
        if(custom_mapping != None):
            new_name_dict = ast.literal_eval(custom_mapping)
        logging.info({"custom":custom_mapping,"ast":new_name_dict})
        result = []
            # Process the job
       
        try:
            result = map_columns_new(file_name, new_name_dict,industry_profile_data, email)
            logging.info(result[0])
         # Update Redis hash value
            if hash_name:
                await redis_client.hset(hash_name, mapping={"taskCompleted": 'false', "inQueue": "false","mapped_cols":json.dumps(result[1])})
                logging.info(f"Updated Redis hash {hash_name}: taskCompleted=True, inQueue=False")
            # Return success response
                return {"success": True,"originalFileName": file_name,"newFileName": result[0],"mapped_cols": result[1]}
        except Exception as e:
            error_details = e.args[0] 
         
            error_message = error_details.get("error_message")
            mapped_cols = error_details.get("mapped_cols")  # Safely access mapped_cols
            logging.info(mapped_cols)
            exc = str(error_message).replace('"', '').replace("'", '')
            logging.error(f"Error in map_columns_new: {exc}")
            if hash_name:
                await redis_client.hset(hash_name, mapping={"taskCompleted": 'false', "inQueue": "false","mapped_cols":json.dumps(mapped_cols)})
                logging.info(f"Updated Redis hash {hash_name}: taskCompleted=false, inQueue=False")
            return {
                "success": False,"originalFileName": file_name,"newFileName": "error","error": "column_mapping_failure","error_details": exc,
                "mapped_cols": mapped_cols}

    except Exception as e:
        logging.error({"errorere":e})
        logging.info(f"Updated Redis hash {hash_name}: taskCompleted=False,sss inQueue=False")
        return {
                "success": False,"originalFileName": file_name,"newFileName": "error","error": "column_mapping_failure","error_details": e,
                "mapped_cols": []}
        


# Main function to manage the worker lifecycle
async def main():
    shutdown_event = asyncio.Event()

    def signal_handler(signal, frame):
        print("Signal received, shutting down.")
        shutdown_event.set()

    # Assign signal handlers for graceful shutdown
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

 

    # Create the worker to process jobs from the queue
    worker = Worker(queue_name, process, {"connection": "redis://default:AVNS_HVGyea8oU5rrDI2oO16@caching-17f435c1-jaiswalk008-project1.h.aivencloud.com:24056"})
    
    # Wait until the shutdown event is set
    await shutdown_event.wait()

    # Clean up the worker and Redis client
    print("Cleaning up worker...")
    await worker.close()
    await redis_client.close()
    print("Worker shut down successfully.")

# Start the worker and handle shutdown gracefully
if __name__ == "__main__":
    asyncio.run(main())
