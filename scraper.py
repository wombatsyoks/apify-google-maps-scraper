"""
Core scraper module for Google Maps
"""
import asyncio
import logging
from typing import List, Dict, Any, Optional
from playwright.async_api import Page, Browser, BrowserContext
from parser import GoogleMapsParser
from utils import random_delay, get_retry_decorator, RateLimiter
from config import GOOGLE_MAPS_URL, TIMING, SELECTORS

logger = logging.getLogger(__name__)


class GoogleMapsScraper:
    """Main scraper class for Google Maps"""
    
    def __init__(self, browser: Browser, proxy_config: Optional[Dict] = None):
        self.browser = browser
        self.proxy_config = proxy_config
        self.context: Optional[BrowserContext] = None
        self.page: Optional[Page] = None
        self.parser: Optional[GoogleMapsParser] = None
        self.rate_limiter = RateLimiter(max_requests=50, time_window=60)
    
    async def initialize(self):
        """Initialize browser context and page"""
        # Create context with proxy if provided
        context_options = {
            'viewport': {'width': 1920, 'height': 1080},
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'locale': 'en-US',
        }
        
        if self.proxy_config:
            context_options['proxy'] = self.proxy_config
        
        self.context = await self.browser.new_context(**context_options)
        self.page = await self.context.new_page()
        self.parser = GoogleMapsParser(self.page)
        
        logger.info("Browser context initialized")
    
    async def close(self):
        """Close browser context"""
        if self.context:
            await self.context.close()
        logger.info("Browser context closed")
    
    async def search(self, query: str, location: str) -> bool:
        """
        Perform search on Google Maps
        
        Args:
            query: Search query (e.g., "coffee shops")
            location: Location (e.g., "New York, NY")
            
        Returns:
            True if search successful, False otherwise
        """
        try:
            # Navigate to Google Maps
            logger.info(f"Navigating to Google Maps...")
            await self.page.goto(GOOGLE_MAPS_URL, wait_until='domcontentloaded')
            await random_delay(3, 5)  # Wait for page to fully load
            
            # Build search query (query + location)
            search_text = f"{query} {location}".strip()
            logger.info(f"Searching for: {search_text}")
            
            # Try multiple selectors for search input (Google Maps can vary)
            search_input_selectors = [
                'input#searchboxinput',
                'input[aria-label*="Search"]',
                'input[name="q"]',
                'input[placeholder*="Search"]'
            ]
            
            search_input = None
            for selector in search_input_selectors:
                try:
                    search_input = await self.page.wait_for_selector(
                        selector, 
                        timeout=5000,
                        state='visible'
                    )
                    if search_input:
                        logger.info(f"Found search input with selector: {selector}")
                        break
                except:
                    continue
            
            if not search_input:
                logger.error("Could not find search input with any selector")
                # Take a screenshot for debugging
                await self.page.screenshot(path='/tmp/gmaps_error.png')
                return False
            
            await search_input.click()
            await random_delay(0.5, 1)
            await search_input.fill(search_text)
            await random_delay(0.5, 1)
            await search_input.press('Enter')
            
            # Wait for results to load
            logger.info("Waiting for search results...")
            await self.page.wait_for_selector(
                SELECTORS['results_container'], 
                timeout=20000
            )
            await random_delay(2, 3)
            
            # Check for no results
            if await self.parser.has_no_results():
                logger.warning("No results found for search query")
                return False
            
            # Check for CAPTCHA
            if await self.parser.has_captcha():
                logger.error("CAPTCHA detected - need manual intervention")
                return False
            
            logger.info("Search completed successfully")
            return True
            
        except Exception as e:
            logger.error(f"Error during search: {e}")
            # Take a screenshot for debugging
            try:
                await self.page.screenshot(path='/tmp/gmaps_error.png')
            except:
                pass
            return False
    
    async def scroll_results(self, max_scrolls: int = 50) -> bool:
        """
        Scroll through results to load more businesses
        
        Args:
            max_scrolls: Maximum number of scroll attempts
            
        Returns:
            True if scrolling completed, False if error
        """
        try:
            results_container = await self.page.query_selector(SELECTORS['results_container'])
            if not results_container:
                logger.error("Results container not found")
                return False
            
            previous_height = await results_container.evaluate('el => el.scrollHeight')
            scroll_count = 0
            no_change_count = 0
            
            logger.info(f"Starting to scroll results (max {max_scrolls} scrolls)...")
            
            while scroll_count < max_scrolls:
                # Scroll to bottom of container
                await results_container.evaluate('el => el.scrollTo(0, el.scrollHeight)')
                await random_delay()
                
                # Wait for content to load
                await self.page.wait_for_timeout(1000)
                
                # Check if new content loaded
                new_height = await results_container.evaluate('el => el.scrollHeight')
                
                if new_height == previous_height:
                    no_change_count += 1
                    if no_change_count >= 3:
                        # No new content after 3 attempts, we've reached the end
                        logger.info(f"Reached end of results after {scroll_count} scrolls")
                        break
                else:
                    no_change_count = 0
                
                previous_height = new_height
                scroll_count += 1
                
                # Check for "You've reached the end" message
                end_message = await self.page.query_selector('text=/reached the end/i')
                if end_message:
                    logger.info("Reached end of results (end message found)")
                    break
            
            return True
            
        except Exception as e:
            logger.error(f"Error during scrolling: {e}")
            return False
    
    async def extract_business_cards(self, max_results: int) -> List[Dict[str, Any]]:
        """
        Extract business data from all visible cards
        
        Args:
            max_results: Maximum number of results to extract
            
        Returns:
            List of business data dictionaries
        """
        businesses = []
        seen_place_ids = set()
        
        try:
            logger.info(f"Extracting business cards (max {max_results})...")
            
            # Get all business cards
            cards = await self.page.query_selector_all(SELECTORS['business_cards'])
            logger.info(f"Found {len(cards)} business cards")
            
            for i, card in enumerate(cards):
                if len(businesses) >= max_results:
                    logger.info(f"Reached max results limit ({max_results})")
                    break
                
                # Rate limiting
                await self.rate_limiter.wait_if_needed()
                
                # Parse card
                business = await self.parser.parse_business_card(card)
                
                if business:
                    # Deduplicate by place ID
                    place_id = business.get('placeId')
                    if place_id and place_id in seen_place_ids:
                        continue
                    
                    if place_id:
                        seen_place_ids.add(place_id)
                    
                    businesses.append(business)
                    
                    if (i + 1) % 10 == 0:
                        logger.info(f"Extracted {len(businesses)} businesses so far...")
            
            logger.info(f"Successfully extracted {len(businesses)} unique businesses")
            return businesses
            
        except Exception as e:
            logger.error(f"Error extracting business cards: {e}")
            return businesses
    
    async def scrape(
        self, 
        query: str, 
        location: str, 
        max_results: int = 100,
        deep_scrape: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Main scraping workflow
        
        Args:
            query: Search query
            location: Location to search
            max_results: Maximum number of results
            deep_scrape: If True, click into each business for details
            
        Returns:
            List of scraped businesses
        """
        try:
            await self.initialize()
            
            # Perform search
            search_success = await self.search(query, location)
            if not search_success:
                return []
            
            # Scroll to load more results
            # Calculate scrolls needed (roughly 10-15 results per scroll)
            needed_scrolls = min(max_results // 10 + 5, 50)
            await self.scroll_results(max_scrolls=needed_scrolls)
            
            # Extract business data
            businesses = await self.extract_business_cards(max_results)
            
            # Deep scrape if requested (click into each business)
            if deep_scrape and businesses:
                logger.info("Deep scraping enabled - extracting detailed info...")
                # This would require clicking each business card
                # Skipping for V1 to keep it simple and fast
                pass
            
            return businesses
            
        except Exception as e:
            logger.error(f"Error in scrape workflow: {e}")
            return []
        
        finally:
            await self.close()
