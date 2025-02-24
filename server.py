from fastapi import FastAPI, Request, File, UploadFile, HTTPException
import logging
from fastapi.responses import PlainTextResponse
import aioboto3
from botocore.config import Config
from dotenv import load_dotenv
import os

load_dotenv()

BUCKET_NAME = os.getenv('BUCKET_NAME')
DOMAIN_NAME = os.getenv('DOMAIN_NAME')
REGION_NAME = os.getenv('REGION_NAME')

logger =logging.getLogger()
logger.setLevel(logging.INFO)

app = FastAPI()

@app.get('/')
async def get_root(request: Request):
    return {
        'message': 'Welcome to face recognition api, send POST with inputFile=img to do face recognition',
        'statusCode': 200
    }

# Clients send in post requests with inputPut file 
@app.post('/')
async def do_face_recognition(inputFile: UploadFile = File(...)):
    try: 
        # Get file name and object
        file_name = inputFile.filename
        file_obj = inputFile.file

        # Store file from request in s3
        await upload_file_to_s3(file_name,file_obj, BUCKET_NAME, REGION_NAME)
        
        # Split file name at the first instance of a .
        item_name, extension = file_name.split('.',1)

        # Perform classification
        query_result = await query_SDB(item_name, DOMAIN_NAME, REGION_NAME)

        # Return HTTP requests
        if query_result:
            # Return a success response
            logger.info(f"face-recognition PASS: response: '{query_result}'")
            return PlainTextResponse(content=query_result, status_code=200)
        else:
            logger.info(f"face-recognition FAIL:  response: '{query_result}'")
            return PlainTextResponse(content=None, status_code=401)
    
    except Exception as e:
        logger.error(f"Error in do_face_recognition():{str(e)}")
        raise HTTPException(status_code=500, detail= {"message":"Internal Server Error while trying facial-recognition"})
    

# ------ Helper functions -------

# Upload given file to an s3 bucket asynchronously  
async def upload_file_to_s3(file_name, file_obj, bucket_name, region_name):

    session = aioboto3.Session()

    config = Config(
        connect_timeout=30, 
        read_timeout=30, 
        retries={'max_attempts': 5, 'mode': 'standard'}
    )
    # async with manages life-cycle of async resource sdb: Makes sure aioboto3 s3 client is closed when finished await without blocking
    async with session.client('s3', region_name=region_name, config=config) as s3:
        try:
            await s3.upload_fileobj(file_obj, bucket_name, file_name) #(Fileobj, Bucket, Key)
            # print(f"Successfully uploaded file '{file_name}'  to bucket '{BUCKET_NAME}'")
            logger.info(f"Successfully uploaded file '{file_name}'  to bucket '{bucket_name}'")
        except Exception as e:
            logger.error(f"Error while trying to upload file '{file_name}'  to bucket '{bucket_name}': {str(e)}")
            raise Exception(f"Error while uploading file '{file_name}' to s3 Bucket '{bucket_name}': {str(e)}")

# Query Simple DB asynchronously
async def query_SDB(file_name, domain_name, region_name):
    session = aioboto3.Session()
    # async with manages life-cycle of async resource sdb: Makes sure its closed when finished await without blocking
    async with session.client('sdb', region_name=region_name) as sdb:
        try:
            response = await sdb.get_attributes(
                        DomainName=domain_name,
                        ItemName=file_name
            )
            if "Attributes" in response:
                result_from_sdb = response['Attributes'][0]["Value"]
                return f"{file_name}:{result_from_sdb}"
            else:
                return None
        except Exception as e:
            logger.error( f"Error while querying SDB {domain_name} with '{file_name}': {str(e)}")
            raise Exception(f"Error while querying SDB {domain_name} with '{file_name}': {str(e)}")




# run multiple instance of your application with: gunicorn -w 3 -k uvicorn.workers.UvicornWorker server:app --bind 0.0.0.0:8000 --log-level info --access-logfile - --access-logformat '%(h)s %(t)s %(r)s %(s)s %(b)s %(L)s'
#   Optimal Number of Workers: A common rule of thumb is to use the number of workers equal to (2 * number of CPU cores) + 1

