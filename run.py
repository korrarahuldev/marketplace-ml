# run.py
import argparse
import logging
import os
import sys
from app.workers.firecrawl_worker import FirecrawlWorker
from app.workers.custom_worker import CustomCrawlerWorker
import uvicorn

def parse_args():
    parser = argparse.ArgumentParser(description="Company Data Ingestion Service")
    parser.add_argument("--mode", choices=["api", "firecrawl", "custom"], default="api", 
                        help="Component to run (api, firecrawl, or custom)")
    parser.add_argument("--port", type=int, default=8000, help="Port for API server")
    parser.add_argument("--config", default="config/config.yaml", 
                        help="Path to config file")
                        
    return parser.parse_args()

def main():
    args = parse_args()

    config_path = os.path.abspath(args.config)
    
    # Verify config file exists
    if not os.path.exists(config_path):
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)
    
    if args.mode == "api":
        # Run FastAPI server
        uvicorn.run("app.main:app", host="0.0.0.0", port=args.port, reload=True)
    
    elif args.mode == "firecrawl":
        # Run Firecrawl worker
        worker = FirecrawlWorker(config_path=config_path)
        worker.run()
    
    elif args.mode == "custom":
        # Run Custom Crawler worker
        worker = CustomCrawlerWorker(config_path=config_path)
        worker.run()

if __name__ == "__main__":
    main()