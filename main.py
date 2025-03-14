import functions_framework
import requests
from bs4 import BeautifulSoup
import re
import datetime
import json
import logging
import os
from google.cloud import storage

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class FedMonitor:
    def __init__(self):
        self.fed_urls = {
            'fomc_statements': 'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm',
            'latest_statement': 'https://www.federalreserve.gov/newsevents/pressreleases.htm',
            'speeches': 'https://www.federalreserve.gov/newsevents/speeches.htm'
        }
        
        
        self.tightening_keywords = [
            'inflation risk', 'price stability', 'overheating', 'tighter', 'hawkish',
            'raise rates', 'rate hike', 'restrictive policy', 'combat inflation',
            'reduce balance sheet', 'quantitative tightening', 'QT', 'upside risk',
            'above target', 'elevated inflation', 'inflation concern',
            'inflation remains elevated', 'inflation has not progressed', 
            'tight labor market', 'upward pressure on prices',
            'higher policy rate', 'increased the target range', 
            'firm policy stance', 'higher for longer', 'will need to remain restrictive',
            'further tightening', 'inflation risks', 'unacceptably high',
            'persistent inflationary pressures', 'further policy firming',
            'commitment to restoring price stability'
        ]
        
        
        self.bucket_name = os.environ.get('GCS_BUCKET_NAME', 'fed-monitor-data')
        self.historical_statements = self.load_historical_data()
        
    def fetch_document(self, url):
        """Fetch and parse a document from a given URL"""
        try:
            logger.info(f"Fetching URL: {url}")
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml'
            }
            response = requests.get(url, headers=headers, timeout=30)
            response.raise_for_status()
            soup = BeautifulSoup(response.text, 'html.parser')
            return soup
        except Exception as e:
            logger.error(f"Error fetching {url}: {e}")
            return None
    
    def extract_statement_links_from_calendar(self):
        """Extract statement links from the FOMC calendar page"""
        logger.info("Extracting statement links from FOMC calendar")
        soup = self.fetch_document(self.fed_urls['fomc_statements'])
        if not soup:
            return []
            
        statement_links = []
        
        try:
            
            statement_indicators = soup.find_all(string=lambda text: text and "Statement:" in text)
            
            for indicator in statement_indicators:
                
                parent = indicator.parent
                
                
                links = parent.find_all('a', href=True, string=lambda s: s and "HTML" in s)
                for link in links:
                    href = link['href']
                    if href.startswith('/'):
                        full_url = f"https://www.federalreserve.gov{href}"
                        statement_links.append(full_url)
                        logger.info(f"Found statement link: {full_url}")
        except Exception as e:
            logger.error(f"Error finding statement links from indicators: {e}")
        
        
        if not statement_links:
            try:
                
                press_links = soup.find_all('a', href=re.compile(r'pressreleases/monetary'))
                
                for link in press_links:
                    if 'statement' in link.get_text().lower() or any(month in link.get_text().lower() for month in ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct', 'nov', 'dec']):
                        href = link['href']
                        if href.startswith('/'):
                            full_url = f"https://www.federalreserve.gov{href}"
                            if full_url not in statement_links:
                                statement_links.append(full_url)
                                logger.info(f"Found press release link: {full_url}")
            except Exception as e:
                logger.error(f"Error finding direct press links: {e}")
        
        
        try:
            latest_soup = self.fetch_document(self.fed_urls['latest_statement'])
            if latest_soup:
                
                press_links = latest_soup.find_all('a', href=re.compile(r'pressreleases/monetary'))
                
                for link in press_links:
                    
                    if 'fomc' in link.get_text().lower() or 'statement' in link.get_text().lower():
                        href = link['href']
                        if href.startswith('/'):
                            full_url = f"https://www.federalreserve.gov{href}"
                            if full_url not in statement_links:
                                statement_links.append(full_url)
                                logger.info(f"Found latest press release: {full_url}")
        except Exception as e:
            logger.error(f"Error checking latest press releases: {e}")
        
        logger.info(f"Total statement links found: {len(statement_links)}")
        return statement_links
    
    def extract_statements(self):
        """Extract and process FOMC policy statements"""
        statement_links = self.extract_statement_links_from_calendar()
        statements = []
        
        for url in statement_links:
            try:
                logger.info(f"Processing statement: {url}")
                soup = self.fetch_document(url)
                if not soup:
                    continue
                
                
                date = self._extract_date(soup)
                if not date:
                    logger.warning(f"Could not extract date from {url}")
                    continue
                
                
                text = self._extract_policy_text(soup)
                if not text or len(text) < 100:  
                    logger.warning(f"Could not extract meaningful text from {url}")
                    continue
                
                logger.info(f"Successfully extracted statement from {date.isoformat()} ({len(text)} chars)")
                statements.append({
                    'date': date.isoformat(), 
                    'text': text, 
                    'url': url
                })
                
            except Exception as e:
                logger.error(f"Error processing statement {url}: {e}")
        
        return statements
    
    def _extract_date(self, soup):
        """Extract date from a Fed document with enhanced reliability"""
        try:
            
            date_elem = soup.find('div', class_='article__time')
            if date_elem and date_elem.text.strip():
                date_text = date_elem.text.strip()
                try:
                    return datetime.datetime.strptime(date_text, '%B %d, %Y')
                except:
                    pass
            
            
            date_elem = soup.find('div', class_='lastUpdate')
            if date_elem and date_elem.text.strip():
                date_text = date_elem.text.strip()
                date_match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', date_text)
                if date_match:
                    month, day, year = date_match.groups()
                    date_str = f"{month} {day}, {year}"
                    return datetime.datetime.strptime(date_str, '%B %d, %Y')
            
            
            release_text = soup.find(string=re.compile(r'For release at|For immediate release'))
            if release_text:
                parent = release_text.parent
                if parent:
                    date_match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', parent.get_text())
                    if date_match:
                        month, day, year = date_match.groups()
                        date_str = f"{month} {day}, {year}"
                        return datetime.datetime.strptime(date_str, '%B %d, %Y')
            
            
            title_elem = soup.find(['h1', 'h2', 'h3', 'h4', 'title'])
            if title_elem:
                title_text = title_elem.text.strip()
                date_match = re.search(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', title_text)
                if date_match:
                    month, day, year = date_match.groups()
                    date_str = f"{month} {day}, {year}"
                    return datetime.datetime.strptime(date_str, '%B %d, %Y')
            
            
            text = soup.get_text()
            date_matches = re.findall(r'([A-Z][a-z]+)\s+(\d{1,2}),?\s+(\d{4})', text)
            if date_matches:
                
                month, day, year = date_matches[0]
                date_str = f"{month} {day}, {year}"
                return datetime.datetime.strptime(date_str, '%B %d, %Y')
                
            return None
        except Exception as e:
            logger.error(f"Error extracting date: {e}")
            return None
    
    def _extract_policy_text(self, soup):
        """Extract the policy-focused text from a Fed statement"""
        try:
            
            content = soup.find('div', class_='col-xs-12 col-sm-8 col-md-8')
            if content:
                
                paragraphs = content.find_all('p')
                
                text = ""
                for p in paragraphs:
                    p_text = p.get_text().strip()
                    if len(p_text) > 100 or "federal funds rate" in p_text.lower() or "monetary policy" in p_text.lower():
                        text += p_text + " "
                
                if text:
                    return text.strip()
            
            
            content = soup.find('div', class_='article__content')
            if content:
                return content.get_text(separator=' ', strip=True)
            
            
            for selector in ['main', 'article', '#content', '.content', '.main-content']:
                content = soup.select_one(selector)
                if content:
                    return content.get_text(separator=' ', strip=True)
            
            
            paragraphs = soup.find_all('p')
            if paragraphs:
                return " ".join([p.get_text().strip() for p in paragraphs])
            
            
            body = soup.find('body')
            if body:
                return body.get_text(separator=' ', strip=True)
                
            return ""
        except Exception as e:
            logger.error(f"Error extracting text: {e}")
            return ""
        
    def extract_policy_decisions(self, text):
        """Extract specific policy decisions from the statement text"""
        decisions = []
        
        
        rate_matches = re.findall(r'(?:decided to|committee will|has decided to|agreed to|decided|appropriate to|increase the target range for the federal funds rate to|decided that the|decided to maintain|will maintain)([^\.;]+)(?:federal funds rate|policy rate|interest rate|interest rates|basis points|percentage point|target range)([^\.;]*)', text, re.IGNORECASE)
        
        for match in rate_matches:
            decision = "".join(match).strip()
            if decision and len(decision) > 10:
                decisions.append(f"Rate Decision: {decision}")
        
        
        balance_sheet_matches = re.findall(r'(?:balance sheet|securities holdings|asset purchases|quantitative|maturity extension|reinvestment|reinvesting|redemptions)([^\.;]{10,150})', text, re.IGNORECASE)
        
        for match in balance_sheet_matches:
            if match and len(match) > 15:
                decisions.append(f"Balance Sheet: {match.strip()}")
        
        
        guidance_matches = re.findall(r'(?:future adjustments|future increases|subsequent meeting|coming months|going forward|remain vigilant|remains highly attentive|future policy|will be prepared to adjust|the committee anticipates|the committee expects|the committee is strongly committed|appropriate path|policy path|outlook|will take into account|in determining)([^\.;]{10,200})', text, re.IGNORECASE)
        
        for match in guidance_matches:
            if match and len(match) > 15:
                decisions.append(f"Forward Guidance: {match.strip()}")
        
        return decisions
    
    def analyze_tightening_signals(self, text):
        """Analyze text for signals of monetary tightening"""
        if not text:
            return 0, [], []
            
        words = text.lower().split()
        text_lower = text.lower()
        
        
        tightening_count = 0
        found_keywords = []
        
        for keyword in self.tightening_keywords:
            if keyword.lower() in text_lower:
                count = text_lower.count(keyword.lower())
                tightening_count += count
                if count > 0:
                    found_keywords.append(keyword)
        
        
        
        direction_change = False
        if re.search(r'(shift|change|pivot).{1,30}(stance|policy|direction)', text_lower):
            direction_change = True
        
        
        comparative_tightening = False
        if re.search(r'(more|increased|stronger|further).{1,20}(restrictive|hawkish|tighten)', text_lower):
            comparative_tightening = True
            tightening_count += 2
        
        
        qt_mentions = len(re.findall(r'(balance sheet reduction|quantitative tightening|qt|runoff|run-off)', text_lower))
        tightening_count += qt_mentions
        
        
        rate_hike = re.search(r'(raise|increase|raising|increasing).{1,30}(federal funds rate|interest rate|target range|policy rate)', text_lower)
        if rate_hike:
            tightening_count += 3
        
        
        policy_decisions = self.extract_policy_decisions(text)
        
        
        text_length = len(words)
        base_score = (tightening_count / max(1, text_length / 100)) * 50  
        
        
        if direction_change:
            base_score += 15
        if comparative_tightening:
            base_score += 20
        if rate_hike:
            base_score += 10
        
        
        easing_language = re.search(r'(lower|decrease|cut|reduce|pause|hold|reduction).{1,30}(federal funds rate|interest rate|target range|policy rate)', text_lower)
        if easing_language:
            base_score -= 20
        
        
        tightening_score = min(100, max(0, base_score))
        
        return tightening_score, found_keywords, policy_decisions
    
    def compare_to_previous(self, current_statement, previous_statement):
        """Compare current statement to previous to detect shifts"""
        if not previous_statement:
            return "No previous statement for comparison"
        
        current_score = current_statement.get('tightening_score', 0)
        previous_score = previous_statement.get('tightening_score', 0)
        
        difference = current_score - previous_score
        
        if difference > 15:
            return f"SIGNIFICANT TIGHTENING SHIFT: +{difference:.1f} points"
        elif difference > 5:
            return f"Moderate tightening shift: +{difference:.1f} points"
        elif difference < -15:
            return f"SIGNIFICANT LOOSENING SHIFT: {difference:.1f} points"
        elif difference < -5:
            return f"Moderate loosening shift: {difference:.1f} points"
        else:
            return f"No significant policy shift: {difference:.1f} points"
    
    def generate_summary(self, statement, comparison_result):
        """Generate a summary of the Fed statement analysis"""
        date_obj = datetime.datetime.fromisoformat(statement['date'])
        formatted_date = date_obj.strftime('%B %d, %Y')
        
        summary = f"Date: {formatted_date}\n"
        summary += f"Tightening Score: {statement['tightening_score']:.1f}/100\n"
        summary += f"Policy Shift: {comparison_result}\n\n"
        
        if 'policy_decisions' in statement and statement['policy_decisions']:
            summary += "Key Policy Decisions:\n"
            for decision in statement['policy_decisions']:
                summary += f"- {decision}\n"
            summary += "\n"
        
        summary += "Tightening Signals:\n"
        for keyword in statement['tightening_keywords']:
            summary += f"- {keyword}\n"
        
        summary += "\nRelevant Excerpts:\n"
        
        sentences = re.split(r'(?<=[.!?])\s+', statement['text'])
        relevant_sentences = []
        
        interest_phrases = self.tightening_keywords + ['federal funds rate', 'monetary policy', 'interest rate', 'target range']
        
        for sentence in sentences:
            for phrase in interest_phrases:
                if phrase.lower() in sentence.lower():
                    clean_sentence = sentence.strip()
                    if clean_sentence and clean_sentence not in relevant_sentences:
                        relevant_sentences.append(clean_sentence)
                        break
        
        
        for i, sentence in enumerate(relevant_sentences[:5]):
            summary += f"{i+1}. {sentence}\n"
        
        summary += f"\nFull statement: {statement['url']}"
        
        return summary
    
    def load_historical_data(self):
        """Load historical statements from Google Cloud Storage"""
        try:
            storage_client = storage.Client()
            try:
                bucket = storage_client.bucket(self.bucket_name)
                blob = bucket.blob('historical_statements.json')
                
                if blob.exists():
                    data = json.loads(blob.download_as_string())
                    logger.info(f"Loaded {len(data)} historical statements")
                    return data
                else:
                    logger.info("No historical data found. Starting fresh.")
                    return []
            except Exception as e:
                logger.warning(f"Error accessing bucket: {e}. Will create new bucket.")
                return []
        except Exception as e:
            logger.error(f"Error loading historical data: {e}")
            return []
    
    def save_historical_data(self):
        """Save historical statements to Google Cloud Storage"""
        try:
            storage_client = storage.Client()
            
            
            try:
                bucket = storage_client.get_bucket(self.bucket_name)
            except Exception:
                bucket = storage_client.create_bucket(self.bucket_name, location="us-central1")
                logger.info(f"Bucket {self.bucket_name} created.")
            
            blob = bucket.blob('historical_statements.json')
            blob.upload_from_string(json.dumps(self.historical_statements))
            logger.info(f"Saved {len(self.historical_statements)} historical statements")
        except Exception as e:
            logger.error(f"Error saving historical data: {e}")
    
    def run_monitoring_cycle(self, force=False):
        """Run a full monitoring cycle and return results"""
        results = {
            'new_statements': [],
            'tightening_alerts': [],
            'status': 'success',
            'debug_info': {
                'start_time': datetime.datetime.now().isoformat(),
                'historical_statements_count': len(self.historical_statements)
            }
        }
        
        try:
            
            statements = self.extract_statements()
            results['debug_info']['statements_found'] = len(statements)
            
            
            existing_dates = {item['date']: item for item in self.historical_statements}
            
            
            for statement in statements:
                
                if force or statement['date'] not in existing_dates:
                    
                    score, keywords, policy_decisions = self.analyze_tightening_signals(statement['text'])
                    
                    
                    new_statement = {
                        'date': statement['date'],
                        'text': statement['text'][:1000] + "..." if len(statement['text']) > 1000 else statement['text'],  
                        'tightening_score': score,
                        'tightening_keywords': keywords,
                        'policy_decisions': policy_decisions,
                        'url': statement['url']
                    }
                    
                    results['new_statements'].append({
                        'date': new_statement['date'],
                        'url': new_statement['url'],
                        'tightening_score': new_statement['tightening_score'],
                        'keywords': new_statement['tightening_keywords'],
                        'policy_decisions': new_statement['policy_decisions']
                    })
                    
                    
                    if not force and statement['date'] not in existing_dates:
                        self.historical_statements.append(new_statement)
                    elif force and statement['date'] in existing_dates:
                        
                        self.historical_statements = [
                            s if s['date'] != statement['date'] else new_statement 
                            for s in self.historical_statements
                        ]
                    
                    
                    if len(self.historical_statements) > 1:
                        
                        sorted_statements = sorted(
                            self.historical_statements, 
                            key=lambda x: x['date'], 
                            reverse=True
                        )
                        
                        
                        previous = None
                        for stmt in sorted_statements:
                            if stmt['date'] < new_statement['date']:
                                previous = stmt
                                break
                        
                        if previous:
                            comparison = self.compare_to_previous(new_statement, previous)
                            
                            
                            if "TIGHTENING SHIFT" in comparison:
                                summary = self.generate_summary(new_statement, comparison)
                                results['tightening_alerts'].append({
                                    'statement_date': new_statement['date'],
                                    'summary': summary
                                })
            
            
            if not force or (force and results['new_statements']):
                self.save_historical_data()
            
            
            results['debug_info']['end_time'] = datetime.datetime.now().isoformat()
            results['debug_info']['new_statements_processed'] = len(results['new_statements'])
            results['debug_info']['final_historical_count'] = len(self.historical_statements)
            
        except Exception as e:
            logger.error(f"Error in monitoring cycle: {e}")
            results['status'] = 'error'
            results['error'] = str(e)
        
        return results

@functions_framework.http
def fed_monitor_http(request):
    """HTTP Cloud Function to monitor Fed statements.
    Args:
        request (flask.Request): The request object.
    Returns:
        The response with monitoring results in JSON format.
    """
    
    request_args = request.args
    force_run = request_args.get('force', 'false').lower() == 'true'
    debug_mode = request_args.get('debug', 'false').lower() == 'true'
    
    logger.info(f"Starting fed_monitor_http function. Force mode: {force_run}, Debug mode: {debug_mode}")
    
    try:
        
        monitor = FedMonitor()
        
        
        results = monitor.run_monitoring_cycle(force=force_run)
        
        
        if not debug_mode and 'debug_info' in results:
            del results['debug_info']
        
        
        response = {
            'timestamp': datetime.datetime.now().isoformat(),
            'results': results
        }
        
        
        return json.dumps(response, indent=2), 200, {'Content-Type': 'application/json'}
    
    except Exception as e:
        error_message = f"Error processing request: {str(e)}"
        logger.error(error_message)
        return json.dumps({
            'status': 'error',
            'error': error_message
        }), 500, {'Content-Type': 'application/json'}