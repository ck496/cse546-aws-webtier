from fastapi import FastAPI, Request, File, UploadFile, HTTPException
import logging
from fastapi.responses import PlainTextResponse
import boto3
from botocore.exceptions import ClientError

logger =logging.getLogger()
logger.setLevel(logging.INFO)
bucket_name = ""
domain_name = ""

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
        upload_file_to_s3(file_name,file_obj, bucket_name)
        
        # Split file name at the first instance of a .
        item_name, extension = file_name.split('.',1)
        # Perform classification
        query_result = query_SDB(item_name, domain_name)

        if query_result:
            # Return a success response
            logger.info({
                "message": f"face-recognition PASS: response: '{query_result}'"
            })
            return PlainTextResponse(content=query_result, status_code=200)
        else:
            logger.info({
                "message": f"face-recognition FAIL:  response: '{query_result}'"
            })
            return PlainTextResponse(content=None, status_code=401)
    
    except Exception as e:
        logger.error({
                "message": "Error in do_face_recognition()",
                "error" : str(e)
            })
        raise HTTPException(status_code=500, detail= {"Internal Server Error while trying facial-recognition"})
    

# ------ Helper functions -------
# Upload given file to an s3 bucket 
def upload_file_to_s3(file_name, file_obj, bucket_name):
    s3 = boto3.client('s3')
    try:
        s3.upload_fileobj(file_obj, bucket_name, file_name) #(Fileobj, Bucket, Key)
        # print(f"Successfully uploaded file '{file_name}'  to bucket '{bucket_name}'")
        # logger.info({
        #     "message": f"Successfully uploaded file '{file_name}'  to bucket '{bucket_name}'"
        # })
    except ClientError as e:
        logger.error({
            "message": f"Error while trying to upload file '{file_name}'  to bucket '{bucket_name}'",
            "error" : str(e)
        })
        raise Exception(f"Error while uploading file '{file_name}' to s3 Bucket '{bucket_name}': {str(e)}")

# Check if a given item exists in simpleDB
def query_SDB(file_name, domain_name):
    # print(f"[DEBUG] querying file '{file_name}' in domain '{domain_name}'")
    try:
        response = boto3.client('sdb').get_attributes(
                    DomainName=domain_name,
                    ItemName=file_name
        )
        if "Attributes" in response:
            result_from_sdb = response['Attributes'][0]["Value"]
            # print(f"[DEBUG] MATCH-FOUND '{file_name}:{result_from_sdb}' in domain '{domain_name}'")
            return f"{file_name}:{result_from_sdb}"
        else:
            # print(f"[DEBUG] NO FOUND for '{file_name}' in domain '{domain_name}'")
            # print(f"[DEBUG] sbd.get_Attributes() response: {response}")
            return None
    except ClientError as e:
        logger.error({
            "message": f"Error while querying SDB {domain_name} with '{file_name}'",
            "error" : str(e)
        })
        raise Exception(f"Error while querying SDB {domain_name} with '{file_name}': {str(e)}")



# run with : uvicorn server:app --reload
