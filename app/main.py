# app/main.py
from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
import uuid
import logging
import yaml
import os
from datetime import datetime
from app.services.queue_service import QueueService
from app.models import CompanyRequest, JobResponse

# Load configuration
with open("config/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger(__name__)

# Initialize FastAPI app
app = FastAPI(title="Company Data Ingestion API")

# Initialize queue service
queue_service = QueueService(config)

@app.post("/api/companies/scrape", response_model=JobResponse)
async def scrape_company(request: CompanyRequest):
    """
    Submit a company for scraping.
    The request will be processed asynchronously.
    """
    try:
        # Validate input
        if not request.company_name or not request.website:
            raise HTTPException(status_code=400, detail="Company name and website are required")
        
        # Create job ID
        job_id = str(uuid.uuid4())
        
        # Prepare job
        job = {
            "job_id": job_id,
            "company_name": request.company_name,
            "website": request.website,
            "status": "pending",
            "created_at": str(datetime.utcnow())
        }
        
        # Send to Firecrawl queue
        success = queue_service.send_to_firecrawl_queue(job)
        
        if not success:
            raise HTTPException(status_code=500, detail="Failed to submit job to queue")
        
        logger.info(f"Job {job_id} submitted for company {request.company_name}")
        
        return JobResponse(
            job_id=job_id,
            status="pending",
            message=f"Job submitted for processing"
        )
        
    except Exception as e:
        logger.error(f"Error submitting job: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/jobs/{job_id}")
async def get_job_status(job_id: str):
    """
    Get the status of a job by ID.
    """
    # This would typically check a database for job status
    # For now, we'll return a placeholder
    return {"job_id": job_id, "status": "pending"}