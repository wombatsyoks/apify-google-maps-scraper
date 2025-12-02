"""
Utility functions for Google Maps scraper
"""
import asyncio
import random
import re
from typing import Optional
from tenacity import retry, stop_after_attempt, wait_exponential
from config import TIMING, RETRY_CONFIG


async def random_delay(min_seconds: float = None, max_seconds: float = None):
    """Add random delay to mimic human behavior"""
    min_s = min_seconds or TIMING['scroll_delay_min']
    max_s = max_seconds or TIMING['scroll_delay_max']
    delay = random.uniform(min_s, max_s)
    await asyncio.sleep(delay)


def get_retry_decorator():
    """Get configured retry decorator"""
    return retry(
        stop=stop_after_attempt(RETRY_CONFIG['max_attempts']),
        wait=wait_exponential(
            multiplier=RETRY_CONFIG['multiplier'],
            min=RETRY_CONFIG['min_wait'],
            max=RETRY_CONFIG['max_wait']
        ),
        reraise=True
    )


def extract_phone(text: str) -> Optional[str]:
    """Extract and normalize phone number"""
    if not text:
        return None
    
    # Remove common non-digit characters except + at start
    cleaned = re.sub(r'[^\d+]', '', text)
    
    # Basic validation
    if len(cleaned) < 10:
        return None
    
    return cleaned


def extract_rating(aria_label: str) -> Optional[float]:
    """Extract rating from aria-label like '4.5 stars'"""
    if not aria_label:
        return None
    
    match = re.search(r'(\d+\.?\d*)\s*star', aria_label, re.IGNORECASE)
    if match:
        try:
            return float(match.group(1))
        except ValueError:
            return None
    return None


def extract_reviews_count(text: str) -> Optional[int]:
    """Extract review count from text like '1,250 reviews'"""
    if not text:
        return None
    
    # Remove commas and extract number
    match = re.search(r'([\d,]+)', text)
    if match:
        try:
            return int(match.group(1).replace(',', ''))
        except ValueError:
            return None
    return None


def extract_coordinates_from_url(url: str) -> Optional[dict]:
    """Extract lat/lng from Google Maps URL"""
    if not url:
        return None
    
    # Pattern: @lat,lng,zoom or !3d[lat]!4d[lng]
    patterns = [
        r'@(-?\d+\.?\d*),(-?\d+\.?\d*)',
        r'!3d(-?\d+\.?\d*)!4d(-?\d+\.?\d*)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            try:
                return {
                    'lat': float(match.group(1)),
                    'lng': float(match.group(2))
                }
            except ValueError:
                continue
    
    return None


def extract_place_id(url: str) -> Optional[str]:
    """Extract place ID from Google Maps URL"""
    if not url:
        return None
    
    # Pattern: /place/... or place_id=...
    patterns = [
        r'place_id=([a-zA-Z0-9_-]+)',
        r'/place/[^/]+/data=.*?1s([a-zA-Z0-9_-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None


def normalize_address(address: str) -> str:
    """Normalize address string"""
    if not address:
        return ""
    
    # Remove extra whitespace
    normalized = re.sub(r'\s+', ' ', address.strip())
    return normalized


def parse_hours(hours_text: str) -> dict:
    """Parse hours of operation from text"""
    hours_dict = {}
    
    if not hours_text:
        return hours_dict
    
    # Split by day
    lines = hours_text.split('\n')
    current_day = None
    
    for line in lines:
        line = line.strip()
        
        # Check if this is a day name
        days = ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday']
        day_found = False
        for day in days:
            if line.startswith(day):
                current_day = day
                # Extract hours part
                hours_part = line.replace(day, '').strip()
                if hours_part:
                    hours_dict[day] = hours_part
                day_found = True
                break
        
        # If not a day line and we have a current day, it might be hours continuation
        if not day_found and current_day and line:
            hours_dict[current_day] = line
    
    return hours_dict


def extract_email_from_text(text: str) -> Optional[str]:
    """Extract email address from text"""
    if not text:
        return None
    
    email_pattern = r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b'
    match = re.search(email_pattern, text)
    
    if match:
        return match.group(0)
    
    return None


class RateLimiter:
    """Simple rate limiter for requests"""
    
    def __init__(self, max_requests: int, time_window: float):
        self.max_requests = max_requests
        self.time_window = time_window
        self.requests = []
    
    async def wait_if_needed(self):
        """Wait if rate limit would be exceeded"""
        now = asyncio.get_event_loop().time()
        
        # Remove old requests outside time window
        self.requests = [req_time for req_time in self.requests 
                        if now - req_time < self.time_window]
        
        # If at limit, wait
        if len(self.requests) >= self.max_requests:
            sleep_time = self.time_window - (now - self.requests[0])
            if sleep_time > 0:
                await asyncio.sleep(sleep_time)
        
        # Add current request
        self.requests.append(now)
