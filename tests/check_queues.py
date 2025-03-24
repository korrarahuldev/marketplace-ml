# check_queues.py
import boto3
import yaml
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def check_sqs_setup():
    """Check SQS queues and configuration"""
    try:
        # Load config
        with open("../config/config.yaml", "r") as f:
            config = yaml.safe_load(f)
        
        # Initialize SQS client
        sqs = boto3.client(
            'sqs',
            region_name=config['aws']['region'],
            aws_access_key_id=config['aws'].get('access_key_id'),
            aws_secret_access_key=config['aws'].get('secret_access_key')
        )
        
        # Check Firecrawl queue
        firecrawl_url = config['aws']['sqs']['firecrawl_queue_url']
        logger.info(f"Checking Firecrawl queue: {firecrawl_url}")
        
        try:
            sqs.get_queue_attributes(
                QueueUrl=firecrawl_url,
                AttributeNames=['All']
            )
            logger.info("✅ Firecrawl queue exists and is accessible")
        except Exception as e:
            logger.error(f"❌ Error accessing Firecrawl queue: {str(e)}")
        
        # Check Custom Crawler queue
        custom_url = config['aws']['sqs']['custom_crawler_queue_url']
        logger.info(f"Checking Custom Crawler queue: {custom_url}")
        
        try:
            sqs.get_queue_attributes(
                QueueUrl=custom_url,
                AttributeNames=['All']
            )
            logger.info("✅ Custom Crawler queue exists and is accessible")
        except Exception as e:
            logger.error(f"❌ Error accessing Custom Crawler queue: {str(e)}")
        
        # List all queues in the account for verification
        all_queues = sqs.list_queues()
        logger.info("\nAll available queues in account:")
        for queue in all_queues.get('QueueUrls', []):
            logger.info(f"- {queue}")
            
    except Exception as e:
        logger.error(f"Error during setup check: {str(e)}")

if __name__ == "__main__":
    check_sqs_setup()