from google.cloud import storage
import os

class GCSConnector:
    """Google Cloud Storage connector"""
    
    def __init__(self, project_id: str, credentials_path: str = None):
        if credentials_path:
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        self.client = storage.Client(project=project_id)
    
    def upload_file(self, bucket_name: str, source_file_path: str, destination_blob_name: str):
        """Upload file to GCS bucket"""
        bucket = self.client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        return f"gs://{bucket_name}/{destination_blob_name}"
    
    def close(self):
        pass  # GCS client doesn't need explicit closing