import os
from fastapi import FastAPI
from google.cloud import storage
from toolz.curried import pipe, map


app = FastAPI()
client = storage.Client()


@app.get("/")
def read_root():
    return {"status": "FastAPI is running on Cloud Run"}

@app.get("/health")
def health_check():
    return {"status": "healthy"}

if __name__ == "__main__":
    import uvicorn
    # Cloud Run provides the PORT env var; default to 8080
    port = int(os.environ.get("PORT", 8080))
    uvicorn.run(app, host="0.0.0.0", port=port)




@app.get("/read-gcs/{bucket_name}/{file_name}")
def read_gcs_file(bucket_name: str, file_name: str):
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(file_name)
    
    content_bytes = blob.download_as_bytes()

    # Using the toolz pipe for your logic
    result = pipe(
        content_bytes,
        decode_bytes,
        shout
    )

    return {"filename": file_name, "processed_content": result}