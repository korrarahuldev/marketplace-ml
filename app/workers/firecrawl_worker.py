# app/workers/firecrawl_worker.py
import json
import logging
import time
from concurrent.futures import ThreadPoolExecutor
import yaml
import os
import sys

# Add the firecrawl scraper to path
sys.path.append(os.path.join(os.path.dirname(__file__), '../../../metadata-extraction-service/firecrawl-scraper'))
from CompanyCrawler_FC import FirecrawlScraper
from app.services.queue_service import QueueService

class FirecrawlWorker:
    def __init__(self, config_path="config/config.yaml"):
        # Load configuration
        self.config_path = config_path
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        # Setup logging
        self.logger = logging.getLogger(__name__)
        
        # Initialize FirecrawlScraper
        self.api_key = self.config['firecrawl']['api_key']
        self.limit = self.config['firecrawl']['limit']
        self.output_dir = self.config['storage']['output_dir']
        
        self.scraper = FirecrawlScraper(
            limit=self.limit,
            api_key=self.api_key,
            output_dir=self.output_dir
        )
        
        # Initialize queue service
        self.queue_service = QueueService(self.config)
        
        # Thread pool size
        self.threadpool_size = self.config['threadpool']['size']
    
    def process_job(self, job):
        """Process a single job using Firecrawl"""
        job_id = job['job_id']
        company_name = job['company_name']
        website = job['website']
        
        self.logger.info(f"Processing job {job_id} for company {company_name}")
        
        try:
            # Use FirecrawlScraper to process the website
            results = self.scraper.process_website(website)
            
            if results:
                # Successfully processed with Firecrawl
                self.logger.info(f"Job {job_id} completed successfully with Firecrawl")
                
                # Add company name to results for data processing
                results['company_name'] = company_name
                
                return True, results
            else:
                # Failed to process with Firecrawl
                self.logger.warning(f"Job {job_id} failed with Firecrawl, sending to custom crawler")
                return False, "Failed to process with Firecrawl"
                
        except Exception as e:
            self.logger.error(f"Error processing job {job_id} with Firecrawl: {str(e)}")
            return False, str(e)
    
    def run(self):
        """Start the worker to process jobs from the queue"""
        self.logger.info(f"Starting Firecrawl worker with threadpool size {self.threadpool_size}")
        
        with ThreadPoolExecutor(max_workers=self.threadpool_size) as executor:
            while True:
                try:
                    # Receive messages from the queue
                    response = self.queue_service.sqs.receive_message(
                        QueueUrl=self.queue_service.firecrawl_queue_url,
                        MaxNumberOfMessages=self.threadpool_size,
                        WaitTimeSeconds=20,
                        AttributeNames=['All']
                    )
                    
                    messages = response.get('Messages', [])
                    
                    if not messages:
                        self.logger.info("No messages in queue, waiting...")
                        time.sleep(5)
                        continue
                    
                    self.logger.info(f"Received {len(messages)} messages from queue")
                    
                    # Process messages in parallel
                    futures = []
                    for message in messages:
                        receipt_handle = message['ReceiptHandle']
                        body = json.loads(message['Body'])
                        
                        # Submit job to threadpool
                        future = executor.submit(self.process_job, body)
                        futures.append((future, receipt_handle, body))
                    
                    # Handle results and delete messages
                    for future, receipt_handle, job in futures:
                        try:
                            success, result = future.result()
                            
                            # Handle the job result
                            if not success:
                                # Send to custom crawler queue if failed
                                self.queue_service.send_to_custom_crawler_queue(job, result)
                            
                            # Delete message from queue
                            self.queue_service.sqs.delete_message(
                                QueueUrl=self.queue_service.firecrawl_queue_url,
                                ReceiptHandle=receipt_handle
                            )
                            
                        except Exception as e:
                            self.logger.error(f"Error processing job: {str(e)}")
                    
                except Exception as e:
                    self.logger.error(f"Error in worker loop: {str(e)}")
                    time.sleep(5)  # Wait before retrying