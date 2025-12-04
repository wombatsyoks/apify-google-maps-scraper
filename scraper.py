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
        # Removed rate limiter for faster scraping
    
    async def initialize(self):
        """Initialize browser context and page"""
        # Create context with proxy if provided
        context_options = {
            'viewport': {'width': 1920, 'height': 1080},
            'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'locale': 'en-US',
            'timezone_id': 'America/New_York',
            'permissions': ['geolocation'],
            'geolocation': {'latitude': 40.7128, 'longitude': -74.0060},  # New York coordinates
            'extra_http_headers': {
                'Accept-Language': 'en-US,en;q=0.9',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
                'sec-ch-ua-mobile': '?0',
                'sec-ch-ua-platform': '"Windows"',
            }
        }
        
        if self.proxy_config:
            context_options['proxy'] = self.proxy_config
        
        self.context = await self.browser.new_context(**context_options)
        
        # Add comprehensive stealth scripts
        await self.context.add_init_script("""
            // Hide webdriver
            Object.defineProperty(navigator, 'webdriver', {
                get: () => undefined
            });
            
            // Override the `plugins` property to use a custom getter.
            Object.defineProperty(navigator, 'plugins', {
                get: () => [1, 2, 3, 4, 5]
            });
            
            // Override the `languages` property to use a custom getter.
            Object.defineProperty(navigator, 'languages', {
                get: () => ['en-US', 'en']
            });
            
            // Override chrome property
            window.chrome = {
                runtime: {}
            };
            
            // Override permissions
            const originalQuery = window.navigator.permissions.query;
            window.navigator.permissions.query = (parameters) => (
                parameters.name === 'notifications' ?
                    Promise.resolve({ state: Notification.permission }) :
                    originalQuery(parameters)
            );
        """)
        
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
        Perform search on Google Maps using direct URL navigation
        
        Args:
            query: Search query (e.g., "coffee shops")
            location: Location (e.g., "New York, NY")
            
        Returns:
            True if search successful, False otherwise
        """
        try:
            # Build search query and construct direct URL
            search_text = f"{query} {location}".strip()
            logger.info(f"Searching for: {search_text}")
            
            # Use direct Google Maps search URL (more reliable than typing)
            import urllib.parse
            encoded_query = urllib.parse.quote(search_text)
            search_url = f"https://www.google.com/maps/search/{encoded_query}"
            
            logger.info(f"Navigating directly to search URL...")
            
            # Navigate with increased timeout and without waiting for full networkidle
            await self.page.goto(
                search_url, 
                wait_until='domcontentloaded',
                timeout=60000  # 60 second timeout
            )
            
            # Give it time to load the results
            await random_delay(4, 6)
            
            # Wait for results to load
            logger.info("Waiting for search results...")
            try:
                await self.page.wait_for_selector(
                    SELECTORS['results_container'], 
                    timeout=30000,
                    state='visible'
                )
                logger.info("Results container found!")
            except Exception as e:
                logger.error(f"Results container not found: {e}")
                # Take screenshot for debugging
                try:
                    await self.page.screenshot(path='/tmp/gmaps_no_results.png')
                    page_content = await self.page.content()
                    logger.info(f"Page title: {await self.page.title()}")
                except:
                    pass
                return False
            
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
                logger.info(f"Page URL: {self.page.url}")
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
                await self.page.wait_for_timeout(500)  # Quick delay for scroll
                
                # Wait for content to load
                await self.page.wait_for_timeout(500)  # Reduced from 1000ms
                
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
                
                # No rate limiting for speed
                
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
    
    async def deep_scrape_businesses(self, businesses: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Click into each business to extract detailed information
        
        Args:
            businesses: List of basic business data
            
        Returns:
            List of businesses with detailed data
        """
        detailed_businesses = []
        
        for i, business in enumerate(businesses):
            try:
                url = business.get('url')
                if not url:
                    detailed_businesses.append(business)
                    continue
                
                logger.info(f"Deep scraping {i+1}/{len(businesses)}: {business.get('title', 'Unknown')}")
                
                # Navigate to business page
                await self.page.goto(url, wait_until='domcontentloaded', timeout=15000)
                await random_delay(2, 3)
                
                # Extract detailed info
                detailed_data = await self.parser.parse_business_details()
                
                # Merge with existing data (detailed data takes priority)
                merged_data = {**business, **detailed_data}
                detailed_businesses.append(merged_data)
                
                if (i + 1) % 5 == 0:
                    logger.info(f"Deep scraped {i+1}/{len(businesses)} businesses...")
                
                # Small delay between businesses
                await self.page.wait_for_timeout(500)
                
            except Exception as e:
                logger.warning(f"Failed to deep scrape {business.get('title', 'Unknown')}: {e}")
                # Keep the basic data if deep scrape fails
                detailed_businesses.append(business)
        
        logger.info(f"Deep scraping completed for {len(detailed_businesses)} businesses")
        return detailed_businesses
    
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
            # Calculate scrolls needed (roughly 5-10 results per scroll)
            needed_scrolls = min(max_results // 5 + 10, 100)  # Increased to get more results
            await self.scroll_results(max_scrolls=needed_scrolls)
            
            # Extract business data
            businesses = await self.extract_business_cards(max_results)
            
            logger.info(f"Total businesses found in cards: {len(businesses)}")
            
            # Deep scrape if requested (click into each business)
            if deep_scrape and businesses:
                logger.info(f"Deep scraping enabled - extracting detailed info for {len(businesses)} businesses...")
                businesses = await self.deep_scrape_businesses(businesses)
            
            return businesses
            
        except Exception as e:
            logger.error(f"Error in scrape workflow: {e}")
            return []
        
        finally:
            await self.close()
