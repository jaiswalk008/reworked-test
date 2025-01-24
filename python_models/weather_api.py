import requests
import pandas as pd
from datetime import datetime, timedelta
import json

class WeatherEventsFetcher:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = "https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline"
        
    def get_weather_events(self, latitude, longitude, days_back=90):
        """
        Fetch weather events for specific coordinates focusing on hail, storm, and high wind
        for the specified number of days back.
        
        Args:
            latitude (float): Location latitude
            longitude (float): Location longitude
            days_back (int): Number of days to look back for weather events
            
        Returns:
            list: List of relevant weather events
        """
        location = f"{latitude},{longitude}"
        
        # Calculate date range
        end_date = datetime.now()  # This will get the current date/time when code runs
        start_date = end_date - timedelta(days=days_back)  # specified number of days
        
        # Format dates for API
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Build the URL with date range
        url = f"{self.base_url}/{location}/{start_date_str}/{end_date_str}"
        
        params = {
            'key': self.api_key,
            'contentType': 'json',
            'include': 'events,days',
            'elements': 'datetime,temp,conditions,description,windspeed,preciptype,cloudcover'
        }
        
        try:
            response = requests.get(url, params=params)
            response.raise_for_status()
            data = response.json()
            
            # Process and filter events
            events = []
            for day in data.get('days', []):
                date = day.get('datetime')
                conditions = day.get('conditions', '').lower()
                description = day.get('description', '').lower()
                wind_speed = day.get('windspeed', 0)
                
                # Check for relevant weather events
                event_found = False
                event_types = []
                
                # Check for hail
                if 'hail' in conditions or 'hail' in description:
                    event_types.append('hail')
                    event_found = True
                
                # Check for storm
                if any(storm_term in conditions or storm_term in description 
                      for storm_term in ['storm', 'thunder', 'lightning']):
                    event_types.append('storm')
                    event_found = True
                
                # Check for high wind (threshold: 20 mph)
                if wind_speed >= 20:
                    event_types.append('high wind')
                    event_found = True
                
                if event_found:
                    events.append({
                        'date': date,
                        'event_types': event_types,
                        'conditions': conditions,
                        'wind_speed': wind_speed,
                        'description': description
                    })
            
            return events
            
        except requests.exceptions.RequestException as e:
            print(f"Error fetching weather data: {e}")
            return []

def test_weather_events():
    # Test coordinates (Durham, NC area)
    test_lat = 30.5084287
    test_long = -91.1245122
    
    # Initialize the fetcher
    api_key = "E33PLV6G3B83PN9L77BVESPN8"
    fetcher = WeatherEventsFetcher(api_key)
    
    # Get weather events
    events = fetcher.get_weather_events(test_lat, test_long, days_back=90)
    
    # Display results
    if events:
        print(f"\nFound {len(events)} relevant weather events in the last {90} days:")
        for event in events:
            print(f"\nDate: {event['date']}")
            print(f"Event Types: {', '.join(event['event_types'])}")
            print(f"Wind Speed: {event['wind_speed']} mph")
            print(f"Conditions: {event['conditions']}")
            print(f"Description: {event['description']}")
    else:
        print("No relevant weather events found in the last {90} days.")

if __name__ == "__main__":
    test_weather_events()