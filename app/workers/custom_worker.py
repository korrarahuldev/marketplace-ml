# app/workers/custom_worker.py
import json
import logging
import time
import yaml
import os
from app.services.custom_crawler import CustomCrawler
from app.services.queue_service import QueueService

class CustomCrawlerWorker:
    def __init__(self, config_path="config/config.yaml"):
        # Setup logging first
        self._setup_logging()
        
        # Load configuration
        self.config_path = config_path  # Store the path
        self.logger.info(f"Loading configuration from {config_path}")
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Initialize custom crawler with config path
        self.logger.info("Initializing custom crawler...")
        self.custom_crawler = CustomCrawler(config_path=self.config_path)
        
        # Initialize queue service
        self.logger.info("Initializing queue service...")
        self.queue_service = QueueService(self.config)
    
    def _setup_logging(self):
        """Configure logging for the worker"""
        # Create logs directory if it doesn't exist
        log_dir = "logs"
        os.makedirs(log_dir, exist_ok=True)
        
        # Setup logging configuration
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(f"{log_dir}/custom_crawler_worker.log"),
                logging.StreamHandler()  # This will print to console
            ]
        )
        self.logger = logging.getLogger(__name__)
        self.logger.info("Logging initialized for Custom Crawler Worker")
    
    def process_job(self, job):
        """Process a job with the custom crawler"""
        job_id = job['job_id']
        company_name = job['company_name']
        website = job['website']
        
        self.logger.info(f"Processing job {job_id} for company {company_name} with custom crawler")
        
        try:
            # Crawl the website
            success, result = self.custom_crawler.crawl_website(website, company_name, job_id)
            
            if success:
                self.logger.info(f"Job {job_id} completed successfully with custom crawler")
                return True, result
            else:
                self.logger.error(f"Job {job_id} failed with custom crawler: {result}")
                return False, result
                
        except Exception as e:
            self.logger.error(f"Error processing job {job_id} with custom crawler: {str(e)}")
            return False, str(e)
    
    def run(self):
        """Start the worker to process jobs from the queue"""
        self.logger.info("Starting Custom Crawler worker")
        self.logger.info(f"Using queue URL: {self.queue_service.custom_crawler_queue_url}")
        
        try:
            while True:
                try:
                    # Receive messages from the queue
                    self.logger.debug("Polling for messages...")
                    response = self.queue_service.sqs.receive_message(
                        QueueUrl=self.queue_service.custom_crawler_queue_url,
                        MaxNumberOfMessages=1,  # Process one at a time due to resource intensity
                        WaitTimeSeconds=20,
                        AttributeNames=['All']
                    )
                    
                    messages = response.get('Messages', [])
                    
                    if not messages:
                        self.logger.info("No messages in custom crawler queue, waiting...")
                        time.sleep(5)
                        continue
                    
                    self.logger.info(f"Received {len(messages)} messages")
                    
                    for message in messages:
                        receipt_handle = message['ReceiptHandle']
                        body = json.loads(message['Body'])
                        
                        self.logger.info(f"Processing message: {body}")
                        
                        # Process the job
                        success, result = self.process_job(body)
                        
                        # Delete the message from the queue
                        self.queue_service.sqs.delete_message(
                            QueueUrl=self.queue_service.custom_crawler_queue_url,
                            ReceiptHandle=receipt_handle
                        )
                        
                        self.logger.info(f"Processed and deleted message from custom crawler queue")
                        
                except Exception as e:
                    self.logger.error(f"Error in message processing loop: {str(e)}")
                    time.sleep(5)  # Wait before retrying
                    
        except KeyboardInterrupt:
            self.logger.info("Received shutdown signal, stopping worker...")
        except Exception as e:
            self.logger.error(f"Fatal error in worker: {str(e)}")
            raise