# app/models.py
from pydantic import BaseModel, HttpUrl
from typing import Optional, List, Dict
from datetime import datetime

class CompanyRequest(BaseModel):
    company_name: str
    website: str

class JobResponse(BaseModel):
    job_id: str
    status: str
    message: Optional[str] = None