import os
import uuid
import functools
import datetime
import requests
import traceback
from typing import Optional, Dict, Any
from contextlib import contextmanager

class PipelineMonitor:
    def __init__(self, pipeline_id: str, job_name: str, api_key: str, job_id:str,client_name:str):
        self.pipeline_id = pipeline_id
        self.job_name = job_name
        self.api_key = api_key
        self.job_id = job_id
        self.client_name = client_name
        self.api_url = "https://admin.mydata.works/api/external/integration-jobs/update_job_status/"
        self.start_time = None
        self.metadata = {}

    def _send_status_update(self, status: str, end_time: Optional[datetime.datetime] = None) -> None:
        payload = {
            "job_id": self.job_id,
            "pipeline_id": self.pipeline_id,
            "job_name": self.job_name,
            "client_name": self.client_name,
            "status": status,
            "start_time": self.start_time.isoformat() if self.start_time else None,
            "end_time": end_time.isoformat() if end_time else None,
            "metadata": self.metadata
        }
        
        # Add all metadata fields at the top level of the payload
        payload["metadata"] = self.metadata

        if status == "FAILED":
            payload["error_message"] = self.metadata.get("error_message")
            payload["error_details"] = self.metadata.get("error_details")

        headers = {
            "x-api-key": self.api_key,
            "Content-Type": "application/json"
        }        
        
        try:
            response = requests.post(self.api_url, json=payload, headers=headers)
            response.raise_for_status()
            print(f"DEBUG: API response status: {response.status_code}")            
        except Exception as e:
            print(f"Failed to send pipeline status update: {e}")

    def update_metadata(self, metadata: Dict[str, Any]) -> None:
        """Update metadata for the current job"""
        self.metadata.update(metadata)

    @contextmanager
    def monitor_execution(self):
        """Context manager for monitoring script execution"""
        self.start_time = datetime.datetime.utcnow()
        self._send_status_update("RUNNING")
        
        try:
            yield self
            end_time = datetime.datetime.utcnow()
            
            # Calculate total duration
            total_duration = (end_time - self.start_time).total_seconds()
            self.metadata.update({
                # "total_duration_seconds": round(total_duration, 2)
            })            
            self._send_status_update("COMPLETED", end_time)
        except Exception as e:
            end_time = datetime.datetime.utcnow()
            
            # Calculate total duration even for failed runs
            total_duration = (end_time - self.start_time).total_seconds()
            
            # Capture error details and add to metadata
            error_message = str(e)
            error_details = traceback.format_exc()
            
            # Add ALL error details to metadata
            self.metadata.update({
                "error_message": error_message,
                "error_details": error_details,
                # "total_duration_seconds": round(total_duration, 2)
            })
            
            print(f"DEBUG: Sending metadata on failure: {self.metadata}")
            # Send status with all error info in metadata
            self._send_status_update("FAILED", end_time)
            
            # Re-raise the exception
            raise e

    def monitor_function(self, func):
        """Decorator for monitoring function execution"""
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            with self.monitor_execution():
                return func(*args, **kwargs)
        return wrapper