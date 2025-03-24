# app/services/queue_service.py
import boto3
import json
import logging
from typing import Dict, Any, Optional

class QueueService:
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.logger = logging.getLogger(__name__)
        
        # Initialize SQS client
        self.sqs = boto3.client(
            'sqs',
            region_name=config['aws']['region'],
            aws_access_key_id=config['aws'].get('access_key_id'),
            aws_secret_access_key=config['aws'].get('secret_access_key')
        )
        
        # Queue URLs
        self.firecrawl_queue_url = config['aws']['sqs']['firecrawl_queue_url']
        self.custom_crawler_queue_url = config['aws']['sqs']['custom_crawler_queue_url']
    
    def send_to_firecrawl_queue(self, job: Dict[str, Any]) -> bool:
        """Send a job to the Firecrawl queue"""
        try:
            response = self.sqs.send_message(
                QueueUrl=self.firecrawl_queue_url,
                MessageBody=json.dumps(job)
            )
            
            self.logger.info(f"Job {job['job_id']} sent to Firecrawl queue with message ID: {response['MessageId']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send job to Firecrawl queue: {str(e)}")
            return False
    
    def send_to_custom_crawler_queue(self, job: Dict[str, Any], failure_reason: Optional[str] = None) -> bool:
        """Send a job to the Custom Crawler queue"""
        if failure_reason:
            job['firecrawl_failure_reason'] = failure_reason
            job['status'] = 'firecrawl_failed'
        
        try:
            response = self.sqs.send_message(
                QueueUrl=self.custom_crawler_queue_url,
                MessageBody=json.dumps(job)
            )
            
            self.logger.info(f"Job {job['job_id']} sent to Custom Crawler queue with message ID: {response['MessageId']}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to send job to Custom Crawler queue: {str(e)}")
            return False