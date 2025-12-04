"""
Parser module for extracting business data from Google Maps
"""
from typing import Optional, Dict, Any
from playwright.async_api import Page, ElementHandle
import logging
from utils import (
    extract_rating, extract_reviews_count, extract_phone,
    extract_coordinates_from_url, extract_place_id, normalize_address,
    parse_hours
)
from config import SELECTORS

logger = logging.getLogger(__name__)


class GoogleMapsParser:
    """Parser for Google Maps business data"""
    
    def __init__(self, page: Page):
        self.page = page
    
    async def parse_business_card(self, card: ElementHandle) -> Optional[Dict[str, Any]]:
        """
        Parse basic information from a business card in search results
        
        Args:
            card: ElementHandle of the business card
            
        Returns:
            Dictionary with business data or None if parsing fails
        """
        try:
            data = {}
            
            # Get the link element which contains most basic info
            link = await card.query_selector('a')
            if not link:
                logger.debug("No link found in card")
                return None
            
            # Get all text content for debugging
            card_text = await card.inner_text()
            logger.debug(f"Card text: {card_text[:100]}...")
            
            # Business name - try multiple selectors
            name_selectors = [
                '[class*="fontHeadlineSmall"]',
                '[class*="fontHeadline"]',
                'div[class*="font"] div:first-child',
                'a div:first-child',
            ]
            
            name_text = None
            for selector in name_selectors:
                try:
                    name_elem = await link.query_selector(selector)
                    if name_elem:
                        name_text = await name_elem.inner_text()
                        if name_text and name_text.strip():
                            data['title'] = name_text.strip()
                            logger.debug(f"Found name with {selector}: {data['title']}")
                            break
                except:
                    continue
            
            # If no name found with selectors, try getting first div text
            if 'title' not in data:
                # Try to extract name from card text (usually first line)
                lines = card_text.split('\n')
                if lines:
                    potential_name = lines[0].strip()
                    # Check if it looks like a business name (not a rating or review count)
                    if potential_name and not potential_name[0].isdigit() and '★' not in potential_name:
                        data['title'] = potential_name
                        logger.debug(f"Extracted name from text: {data['title']}")
            
            if 'title' not in data:
                logger.debug("Could not find business name, skipping card")
                return None
            
            # Rating - look for star rating
            rating_elem = await card.query_selector('span[role="img"]')
            if rating_elem:
                aria_label = await rating_elem.get_attribute('aria-label')
                if aria_label:
                    data['rating'] = extract_rating(aria_label)
                    logger.debug(f"Found rating: {data.get('rating')}")
            
            # Reviews count - look for text with numbers and "reviews"
            card_html = await card.inner_html()
            if 'review' in card_text.lower():
                data['reviewsCount'] = extract_reviews_count(card_text)
                logger.debug(f"Found reviews count: {data.get('reviewsCount')}")
            
            # Category and other info from text
            lines = card_text.split('\n')
            for line in lines[1:]:  # Skip first line (name)
                line = line.strip()
                if not line:
                    continue
                    
                # Category (usually contains words, not numbers)
                if 'category' not in data and '·' in line:
                    parts = line.split('·')
                    if parts:
                        category = parts[0].strip()
                        if category and not category[0].isdigit():
                            data['category'] = category
                            logger.debug(f"Found category: {category}")
                
                # Price level (contains $)
                if '$' in line and 'priceLevel' not in data:
                    # Extract just the $ symbols
                    price_part = next((part.strip() for part in line.split('·') if '$' in part), None)
                    if price_part:
                        data['priceLevel'] = price_part
                        logger.debug(f"Found price level: {price_part}")
            
            # Get URL for place ID
            href = await link.get_attribute('href')
            if href:
                data['url'] = f"https://www.google.com{href}" if href.startswith('/') else href
                data['placeId'] = extract_place_id(data['url'])
                logger.debug(f"Found URL: {data['url']}")
                
                # Try to get coordinates from URL
                coords = extract_coordinates_from_url(data['url'])
                if coords:
                    data['coordinates'] = coords
                    logger.debug(f"Found coordinates: {coords}")
            
            logger.info(f"Successfully parsed: {data.get('title', 'Unknown')}")
            return data
            
        except Exception as e:
            logger.error(f"Error parsing business card: {e}", exc_info=True)
            return None
    
    async def parse_business_details(self, deep_scrape: bool = False) -> Dict[str, Any]:
        """
        Parse detailed information from business detail page
        
        Args:
            deep_scrape: If True, extract additional details like reviews
            
        Returns:
            Dictionary with detailed business data
        """
        data = {}
        
        try:
            # Wait for content to load
            await self.page.wait_for_selector('h1', timeout=5000)
            
            # Business name
            name = await self.page.query_selector('h1')
            if name:
                data['title'] = await name.inner_text()
            
            # Rating and reviews
            rating_elem = await self.page.query_selector('span[role="img"][aria-label*="star"]')
            if rating_elem:
                aria_label = await rating_elem.get_attribute('aria-label')
                data['rating'] = extract_rating(aria_label)
            
            reviews_elem = await self.page.query_selector('button[aria-label*="review"]')
            if reviews_elem:
                reviews_text = await reviews_elem.inner_text()
                data['reviewsCount'] = extract_reviews_count(reviews_text)
            
            # Address
            address_button = await self.page.query_selector('button[data-item-id*="address"]')
            if address_button:
                address_text = await address_button.inner_text()
                data['address'] = normalize_address(address_text)
            
            # Phone
            phone_button = await self.page.query_selector('button[data-item-id*="phone"]')
            if phone_button:
                phone_text = await phone_button.inner_text()
                data['phone'] = extract_phone(phone_text)
            
            # Website
            website_link = await self.page.query_selector('a[data-item-id*="authority"]')
            if website_link:
                data['website'] = await website_link.get_attribute('href')
            
            # Category/Type
            category_button = await self.page.query_selector('button[jsaction*="category"]')
            if category_button:
                data['category'] = await category_button.inner_text()
            
            # Hours
            hours_button = await self.page.query_selector('button[aria-label*="Hours"]')
            if hours_button:
                # Click to expand hours
                await hours_button.click()
                await self.page.wait_for_timeout(500)
                
                hours_container = await self.page.query_selector('div[aria-label*="Hours"]')
                if hours_container:
                    hours_text = await hours_container.inner_text()
                    data['hours'] = parse_hours(hours_text)
            
            # Plus Code
            plus_code = await self.page.query_selector('button[data-item-id*="oloc"]')
            if plus_code:
                data['plusCode'] = await plus_code.inner_text()
            
            # Get current URL for place ID and coordinates
            current_url = self.page.url
            data['url'] = current_url
            data['placeId'] = extract_place_id(current_url)
            
            coords = extract_coordinates_from_url(current_url)
            if coords:
                data['coordinates'] = coords
            
            # Deep scrape: reviews
            if deep_scrape:
                # This would require additional clicking and parsing
                # Placeholder for now
                data['reviews'] = []
            
            return data
            
        except Exception as e:
            logger.error(f"Error parsing business details: {e}")
            return data
    
    async def has_captcha(self) -> bool:
        """Check if CAPTCHA is present"""
        try:
            captcha = await self.page.query_selector('iframe[src*="recaptcha"]')
            return captcha is not None
        except:
            return False
    
    async def has_no_results(self) -> bool:
        """Check if search returned no results"""
        try:
            no_results = await self.page.query_selector('text="No results found"')
            return no_results is not None
        except:
            return False
