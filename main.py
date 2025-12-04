"""
Google Maps Business Scraper - Apify Actor
Production-grade scraper for extracting business data from Google Maps
"""
import asyncio
import logging
from datetime import datetime
from typing import Dict, Any
from apify import Actor
from playwright.async_api import async_playwright
from scraper import GoogleMapsScraper
from config import DEFAULTS

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def main():
    """Main entry point for the Apify actor"""
    async with Actor:
        # Get input
        actor_input = await Actor.get_input() or {}
        logger.info(f"Actor input: {actor_input}")
        
        # Validate and parse input
        search_query = actor_input.get('searchQuery')
        location = actor_input.get('location')
        
        if not search_query:
            raise ValueError("searchQuery is required")
        if not location:
            raise ValueError("location is required")
        
        max_results = actor_input.get('maxResults', DEFAULTS['max_results'])
        deep_scrape = actor_input.get('deepScrape', DEFAULTS['deep_scrape'])
        include_reviews = actor_input.get('includeReviews', DEFAULTS['include_reviews'])
        proxy_config = actor_input.get('proxyConfig', {})
        
        # Log configuration
        logger.info(f"Search query: {search_query}")
        logger.info(f"Location: {location}")
        logger.info(f"Max results: {max_results}")
        logger.info(f"Deep scrape: {deep_scrape}")
        
        # Configure Apify proxy if enabled
        playwright_proxy = None
        if proxy_config.get('useApifyProxy', True):
            proxy_url = await Actor.create_proxy_configuration()
            if proxy_url:
                playwright_proxy = {
                    'server': await proxy_url.new_url(),
                }
                logger.info("Apify proxy configured")
        
        # Initialize Playwright
        logger.info("Starting Playwright...")
        async with async_playwright() as playwright:
            # Launch browser
            browser = await playwright.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--disable-dev-shm-usage',
                    '--no-sandbox',
                ]
            )
            
            try:
                # Create scraper instance
                scraper = GoogleMapsScraper(browser, proxy_config=playwright_proxy)
                
                # Run scraping
                logger.info("Starting scraping process...")
                businesses = await scraper.scrape(
                    query=search_query,
                    location=location,
                    max_results=max_results,
                    deep_scrape=deep_scrape
                )
                
                # Add scraping metadata
                scraped_at = datetime.utcnow().isoformat() + 'Z'
                for business in businesses:
                    business['scrapedAt'] = scraped_at
                    business['searchQuery'] = search_query
                    business['searchLocation'] = location
                
                # Push data to Apify dataset
                logger.info(f"Saving {len(businesses)} businesses to dataset...")
                await Actor.push_data(businesses)
                
                # Set output statistics
                await Actor.set_value('OUTPUT', {
                    'totalResults': len(businesses),
                    'searchQuery': search_query,
                    'location': location,
                    'scrapedAt': scraped_at,
                })
                
                logger.info(f"✅ Successfully scraped {len(businesses)} businesses")
                
            except Exception as e:
                logger.error(f"❌ Error during scraping: {e}", exc_info=True)
                raise
            
            finally:
                await browser.close()
                logger.info("Browser closed")


if __name__ == '__main__':
    asyncio.run(main())
