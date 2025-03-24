# src/custom_crawler/crawler.py
import json
import logging
import os
import time
import yaml
import boto3
import requests
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
import markdown
import re
from pathlib import Path


try:
    from playwright.sync_api import sync_playwright
    PLAYWRIGHT_AVAILABLE = True
except ImportError:
    PLAYWRIGHT_AVAILABLE = False

try:
    from selenium import webdriver
    from selenium.webdriver.chrome.service import Service
    from selenium.webdriver.chrome.options import Options
    from webdriver_manager.chrome import ChromeDriverManager
    SELENIUM_AVAILABLE = True
except ImportError:
    SELENIUM_AVAILABLE = False

class CustomCrawler:
    def __init__(self, config_path="config/config.yaml"):
        with open(config_path, 'r') as file:
            self.config = yaml.safe_load(file)
        
        self.browser_type = self.config['custom_crawler']['browser']
        self.headless = self.config['custom_crawler']['headless']
        self.timeout = self.config['custom_crawler']['timeout']
        self.max_pages = self.config['custom_crawler']['max_pages']
        self.respect_robots_txt = self.config['custom_crawler']['respect_robots_txt']
        
        self.pdf_folder = self.config['storage']['pdf_folder']
        self.html_folder = self.config['storage']['html_folder']
        self.markdown_folder = self.config['storage']['markdown_folder']
        
        self.sqs = boto3.client('sqs', region_name=self.config['aws']['region'])
        self.custom_crawler_queue_url = self.config['aws']['sqs']['custom_crawler_queue_url']
        
        self.logger = logging.getLogger(__name__)
        self.excluded_patterns = self.config['custom_crawler'].get('excluded_patterns', [])
        self.excluded_patterns = [re.compile(pattern) for pattern in self.excluded_patterns]
        
         # Patterns for image URLs to collect but not scrape
        self.image_patterns = [
            r'\.(jpg|jpeg|png|gif)$'
        ]
        self.image_patterns = [re.compile(pattern) for pattern in self.image_patterns]

        # Create storage directories if they don't exist
        os.makedirs(self.pdf_folder, exist_ok=True)
        os.makedirs(self.html_folder, exist_ok=True)
        os.makedirs(self.markdown_folder, exist_ok=True)
    
    def get_robots_txt_rules(self, website):
        """Parse robots.txt file to get crawling rules"""
        parsed_url = urlparse(website)
        robots_url = f"{parsed_url.scheme}://{parsed_url.netloc}/robots.txt"
        
        try:
            response = requests.get(robots_url, timeout=10)
            if response.status_code == 200:
                # Very basic robots.txt parsing - in production you'd use a proper parser
                disallowed_paths = []
                for line in response.text.split('\n'):
                    if line.lower().startswith('disallow:'):
                        path = line.split(':', 1)[1].strip()
                        if path:
                            disallowed_paths.append(path)
                
                return disallowed_paths
            else:
                return []
        except Exception as e:
            self.logger.warning(f"Failed to fetch robots.txt: {str(e)}")
            return []
    
    def is_allowed_url(self, url, disallowed_paths):
        """Check if URL is allowed based on robots.txt rules"""
        if not self.respect_robots_txt or not disallowed_paths:
            return True
        
        parsed_url = urlparse(url)
        path = parsed_url.path
        
        for disallowed in disallowed_paths:
            if path.startswith(disallowed):
                return False
        
        return True
    
    def setup_browser(self):
        """Set up the browser based on configuration"""
        if self.browser_type == "playwright" and PLAYWRIGHT_AVAILABLE:
            playwright = sync_playwright().start()
            browser = playwright.chromium.launch(headless=self.headless)
            context = browser.new_context(
                user_agent="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
            )
            page = context.new_page()
            page.set_default_timeout(self.timeout * 1000)  # Convert to milliseconds
            return {
                "type": "playwright",
                "playwright": playwright,
                "browser": browser,
                "context": context,
                "page": page
            }
        elif self.browser_type == "selenium" and SELENIUM_AVAILABLE:
            options = Options()
            if self.headless:
                options.add_argument("--headless")
            options.add_argument("--no-sandbox")
            options.add_argument("--disable-dev-shm-usage")
            options.add_argument("--disable-gpu")
            options.add_argument("--window-size=1920,1080")
            options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
            
            service = Service(ChromeDriverManager().install())
            driver = webdriver.Chrome(service=service, options=options)
            driver.set_page_load_timeout(self.timeout)
            
            return {
                "type": "selenium",
                "driver": driver
            }
        else:
            raise ValueError(f"Browser type {self.browser_type} not available or not supported")
    
    def close_browser(self, browser_context):
        """Close the browser properly"""
        if browser_context["type"] == "playwright":
            browser_context["page"].close()
            browser_context["browser"].close()
            browser_context["playwright"].stop()
        elif browser_context["type"] == "selenium":
            browser_context["driver"].quit()
    
    def get_page_content(self, url, browser_context):
        """Get the content of a page using the configured browser"""
        try:
            if browser_context["type"] == "playwright":
                page = browser_context["page"]
                page.goto(url)
                # Wait for network to be idle (no more than 2 requests in flight)
                page.wait_for_load_state("networkidle")
                content = page.content()
                return content
            elif browser_context["type"] == "selenium":
                driver = browser_context["driver"]
                driver.get(url)
                # Wait for page to load
                time.sleep(2)
                content = driver.page_source
                return content
        except Exception as e:
            self.logger.error(f"Error getting content for URL {url}: {str(e)}")
            return None
    
    def extract_links(self, content, base_url):
        """Extract links from HTML content"""
        soup = BeautifulSoup(content, 'html.parser')
        links = []
        
        # Extract regular links
        for a_tag in soup.find_all('a', href=True):
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            links.append(full_url)
        
        # Extract PDF links
        for a_tag in soup.find_all('a', href=lambda href: href and href.lower().endswith('.pdf')):
            href = a_tag['href']
            full_url = urljoin(base_url, href)
            links.append(full_url)
        
        return links
    
    def is_same_domain(self, url, base_domain):
        """Check if URL is from the same domain"""
        parsed_url = urlparse(url)
        return parsed_url.netloc == base_domain
    
    def download_pdf(self, url, company_name, job_id):
        """Download a PDF file"""
        try:
            response = requests.get(url, timeout=30)
            if response.status_code == 200 and 'application/pdf' in response.headers.get('Content-Type', ''):
                # Create company-specific folder
                company_folder = os.path.join(self.pdf_folder, f"{company_name}_{job_id}")
                os.makedirs(company_folder, exist_ok=True)
                
                # Extract filename from URL or use a default
                filename = os.path.basename(urlparse(url).path) or f"document_{int(time.time())}.pdf"
                filepath = os.path.join(company_folder, filename)
                
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                
                self.logger.info(f"Downloaded PDF: {filepath}")
                return filepath
            else:
                self.logger.warning(f"Failed to download PDF from {url}: Status code {response.status_code}")
                return None
        except Exception as e:
            self.logger.error(f"Error downloading PDF from {url}: {str(e)}")
            return None
    
    def save_html(self, content, url, company_name, job_id):
        """Save HTML content to file"""
        try:
            # Create company-specific folder
            company_folder = os.path.join(self.html_folder, f"{company_name}_{job_id}")
            os.makedirs(company_folder, exist_ok=True)
            
            # Create a filename based on the URL
            parsed_url = urlparse(url)
            path = parsed_url.path.strip('/')
            if not path:
                path = 'index'
            
            # Replace invalid filename characters
            filename = re.sub(r'[^\w\-_.]', '_', path) + '.html'
            filepath = os.path.join(company_folder, filename)
            
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(content)
            
            self.logger.info(f"Saved HTML: {filepath}")
            return filepath
        except Exception as e:
            self.logger.error(f"Error saving HTML for {url}: {str(e)}")
            return None
    
    def convert_html_to_markdown(self, html_path):
        """Convert HTML file to Markdown"""
        try:
            # Read HTML file
            with open(html_path, 'r', encoding='utf-8') as f:
                html_content = f.read()
            
            # Parse HTML with BeautifulSoup
            soup = BeautifulSoup(html_content, 'html.parser')
            
            # Extract main content (remove scripts, styles, etc.)
            for tag in soup(['script', 'style', 'meta', 'link']):
                tag.decompose()
            
            # Convert to markdown
            html_clean = str(soup)
            md_content = self.html_to_markdown(html_clean)
            
            # Create markdown path
            html_path = Path(html_path)
            company_folder = html_path.parent.name
            md_folder = os.path.join(self.markdown_folder, company_folder)
            os.makedirs(md_folder, exist_ok=True)
            
            md_path = os.path.join(md_folder, html_path.stem + '.md')
            
            # Save markdown file
            with open(md_path, 'w', encoding='utf-8') as f:
                f.write(md_content)
            
            self.logger.info(f"Converted HTML to Markdown: {md_path}")
            return md_path
        except Exception as e:
            self.logger.error(f"Error converting HTML to Markdown for {html_path}: {str(e)}")
            return None
    
    def html_to_markdown(self, html_content):
        """Convert HTML to Markdown using a simple approach"""
        # This is a simplified version - in production you might want to use a more robust solution
        # like html2text or a custom BeautifulSoup parser
        soup = BeautifulSoup(html_content, 'html.parser')
        
        # Extract text
        text = soup.get_text(separator='\n')
        
        # Clean up whitespace
        text = re.sub(r'\n\s*\n', '\n\n', text)
        text = re.sub(r' +', ' ', text)
        
        return text
    
    def crawl_website(self, website, company_name, job_id):
        """Crawl a website and extract content"""
        self.logger.info(f"Starting crawl for {website}")
        
        # Parse base domain for same-domain check
        parsed_url = urlparse(website)
        base_domain = parsed_url.netloc
        
        # Get robots.txt rules if configured
        disallowed_paths = []
        if self.respect_robots_txt:
            disallowed_paths = self.get_robots_txt_rules(website)
        
        # Setup browser
        browser_context = self.setup_browser()
        
        try:
            # Initialize crawl
            visited_urls = set()
            to_visit = [website]
            pdf_files = []
            html_files = []
            markdown_files = []
            
            # Start crawling
            page_count = 0
            
            while to_visit and page_count < self.max_pages:
                current_url = to_visit.pop(0)
                
                # Skip if already visited or not allowed
                if current_url in visited_urls or not self.is_allowed_url(current_url, disallowed_paths):
                    continue
                
                visited_urls.add(current_url)
                page_count += 1
                
                self.logger.info(f"Crawling page {page_count}/{self.max_pages}: {current_url}")
                
                # Handle PDF files
                if current_url.lower().endswith('.pdf'):
                    pdf_path = self.download_pdf(current_url, company_name, job_id)
                    if pdf_path:
                        pdf_files.append(pdf_path)
                    continue
                
                # Get page content
                content = self.get_page_content(current_url, browser_context)
                if not content:
                    continue
                
                # Save HTML
                html_path = self.save_html(content, current_url, company_name, job_id)
                if html_path:
                    html_files.append(html_path)
                    
                    # Convert to markdown
                    md_path = self.convert_html_to_markdown(html_path)
                    if md_path:
                        markdown_files.append(md_path)
                
                # Extract links and add to queue
                links = self.extract_links(content, current_url)
                for link in links:
                    if (link not in visited_urls and 
                        link not in to_visit and 
                        self.is_same_domain(link, base_domain) and
                        self.is_allowed_url(link, disallowed_paths)):
                        to_visit.append(link)
            
                        
            self.logger.info(f"Crawl completed for {website}. Visited {len(visited_urls)} pages.")
            
            # Return results
            results = {
                "company_name": company_name,
                "job_id": job_id,
                "website": website,
                "pages_crawled": len(visited_urls),
                "pdf_files": pdf_files,
                "html_files": html_files,
                "markdown_files": markdown_files
            }
            
            return True, results
            
        except Exception as e:
            self.logger.error(f"Error during crawl of {website}: {str(e)}")
            return False, str(e)
        finally:
            # Always close the browser
            self.close_browser(browser_context)
    
    def process_job(self, job):
        """Process a job from the custom crawler queue"""
        job_id = job['job_id']
        company_name = job['company_name']
        website = job['website']
        
        self.logger.info(f"Processing job {job_id} for company {company_name} with custom crawler")
        
        # Crawl the website
        success, result = self.crawl_website(website, company_name, job_id)
        
        if success:
            # Send to data processing pipeline
            # This is where you would integrate with your existing data processing implementation
            self.logger.info(f"Job {job_id} completed successfully with custom crawler")
            
            # TODO: Call your data processing pipeline here with the result
            # Example: data_processor.process_crawl_results(result)
            
            return True
        else:
            self.logger.error(f"Job {job_id} failed with custom crawler: {result}")
            return False
    
    def start_worker(self):
        """Start the worker to process jobs from the custom crawler queue"""
        self.logger.info("Starting Custom Crawler worker")
        
        while True:
            try:
                # Receive messages from the queue
                response = self.sqs.receive_message(
                    QueueUrl=self.custom_crawler_queue_url,
                    MaxNumberOfMessages=1,  # Process one at a time due to resource intensity
                    WaitTimeSeconds=20,
                    AttributeNames=['All']
                )
                
                messages = response.get('Messages', [])
                
                if not messages:
                    self.logger.info("No messages in custom crawler queue, waiting...")
                    continue
                
                for message in messages:
                    receipt_handle = message['ReceiptHandle']
                    body = json.loads(message['Body'])
                    
                    # Process the job
                    success = self.process_job(body)
                    
                    # Delete the message from the queue
                    self.sqs.delete_message(
                        QueueUrl=self.custom_crawler_queue_url,
                        ReceiptHandle=receipt_handle
                    )
                    
                    self.logger.info(f"Processed and deleted message from custom crawler queue")
                    
            except Exception as e:
                self.logger.error(f"Error in custom crawler worker loop: {str(e)}")
                time.sleep(5)  # Wait before retrying