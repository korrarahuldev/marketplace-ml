from typing import Dict, List, Tuple, Optional
import csv
import logging
from datetime import datetime
from pathlib import Path
import os
from firecrawl import FirecrawlApp

class FirecrawlScraper:
    """
    A class to handle website scraping operations using Firecrawl API.
    """
    
    def __init__(self, limit: int = 2, api_key: str = None, output_dir: str = "scraped_data"):
        """
        Initialize FirecrawlScraper with API key and output directory.
        
        Args:
            api_key (str): Firecrawl API key
            output_dir (str): Directory to store output files
        """
        self.app = FirecrawlApp(api_key=api_key)
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        self.limit = limit
        # Setup logging
        self._setup_logging()
        
    def _setup_logging(self):
        """Configure logging for the scraper."""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'scraper.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def _generate_filename(self, url: str, suffix: str) -> str:
        """Generate a filename based on URL and timestamp."""
        # timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        safe_url = url.replace('https://', '').replace('http://', '').replace('/', '_')[:30]
        return f"{safe_url}_{suffix}"

    def scrape_single_url(self, url: str) -> Dict:
        """
        Scrape a single URL and return the results.
        
        Args:
            url (str): URL to scrape
            
        Returns:
            Dict: Scraping results
        """
        try:
            self.logger.info(f"Starting single URL scrape for: {url}")
            scrape_status = self.app.scrape_url(
                url,
                params={'formats': ['markdown'],'onlyMainContent': True}
            )
            self.logger.info(f"Successfully scraped URL: {url}")
            return scrape_status
        except Exception as e:
            self.logger.error(f"Error scraping URL {url}: {str(e)}")
            raise
    
    def get_website_map(self, url: str) -> Tuple[List[str], int]:
        """
        Get website map and count of pages.
        
        Args:
            url (str): Website URL
            
        Returns:
            Tuple[List[str], int]: List of URLs and total count
        """
        try:
            self.logger.info(f"Mapping website: {url}")
            map_result = self.app.map_url(url)
            links = map_result['links']
            count = len(links)
            self.logger.info(f"Found {count} URLs on {url}")
            return links, count
        except Exception as e:
            self.logger.error(f"Error mapping website {url}: {str(e)}")
            raise

    def crawl_website(self, url: str) -> Dict:
        """
        Crawl website with specified limit.
        
        Args:
            url (str): Website URL
            limit (int): Maximum number of pages to crawl
            
        Returns:
            Dict: Crawl results
        """
        try:
            self.logger.info(f"Starting crawl for {url} with limit {self.limit}")
            crawl_status = self.app.crawl_url(
                url,
                params={
                    'limit': self.limit,
                    'scrapeOptions': {'formats': ['markdown', 'html']},
                    'excludePaths': ['/news/', '/Blog*','/blog*','/tag/','/category/']
                }
            )
            
            self.logger.info(f"Successfully crawled {url}")
            return crawl_status
        except Exception as e:
            self.logger.error(f"Error crawling website {url}: {str(e)}")
            raise
    
    def save_to_csv(self, crawl_result: Dict, url: str) -> str:
        """
        Save crawl results to CSV file.
        
        Args:
            crawl_result (Dict): Crawl results to save
            url (str): Source URL
            
        Returns:
            str: Path to saved CSV file
        """
        try:
            filename = self._generate_filename(url, '.csv')
            output_file = self.output_dir / filename
            
            fieldnames = ['source_url', 'markdown_text','source_type']
            data_to_write = []

            for entry in crawl_result['data']:
                try:
                    data_to_write.append({
                        'source_url': entry['metadata']['sourceURL'],
                        'markdown_text': entry['markdown'],
                        'source_type': 'company_website'
                    })
                except KeyError as e:
                    self.logger.warning(f"Skipping entry due to missing key: {e}")
                except Exception as e:
                    self.logger.warning(f"Error processing entry: {e}")

            with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data_to_write)

            self.logger.info(f"Saved {len(data_to_write)} entries to {output_file}")
            return str(output_file)
        except Exception as e:
            self.logger.error(f"Error saving CSV file: {str(e)}")
            raise

    def process_website(self, url: str, limit: Optional[int] = 3) -> Dict:
        """
        Complete website processing pipeline: map, crawl, and save results.
        
        Args:
            url (str): Website URL
            limit (Optional[int]): Maximum pages to crawl
            
        Returns:
            Dict: Processing results including file paths and statistics
        """
        try:
            self.logger.info(f"Starting complete website processing for {url}")
            
            # Get website map
            # links, total_pages = self.get_website_map(url)
            
            # Crawl website
            crawl_results = self.crawl_website(url)
            
            # Save results
            csv_path = self.save_to_csv(crawl_results, url)
            
            results = {
                'url': url,
                # 'total_pages_found': total_pages,
                'pages_crawled': len(crawl_results['data']),
                'csv_path': csv_path,
                'timestamp': datetime.now().isoformat()
            }
            
            self.logger.info(f"Completed processing for {url}")
            return results
            
        except Exception as e:
            self.logger.error(f"Error in complete processing of {url}: {str(e)}")
            raise

# def main():
#     # Example usage
#     # api_key = os.environ.get('FIRECRAWL_API_KEY_PROD')
#     FIRECRAWL_API_KEY_PROD = 'fc-c01463181b934cf5bd67cd41e75f83b8'
#     FIRECRAWL_API_KEY_DEV = 'fc-3f748292413444f3b60dd8e434ee415e'
#     api_key = FIRECRAWL_API_KEY_PROD
    
#     if not api_key:
#         raise ValueError("FIRECRAWL_API_KEY environment variable not set")
    
#     scraper = FirecrawlScraper(api_key=api_key)
    
#     test_url = "http://www.aakri.in"
#     try:
#         results = scraper.process_website(test_url)
#         print("Processing completed successfully:")
#         for key, value in results.items():
#             print(f"{key}: {value}")
#     except Exception as e:
#         print(f"Error processing website: {str(e)}")

# if __name__ == "__main__":
#     main()