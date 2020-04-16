"""This module provides constant settings
"""

# Time zone to use
TZ = 'UTC'

# Start and end dates
START = "2018-01-01"
END = "2019-01-01"

AREAS = {
  # Task 25 countries in Europe
 'DK': 'Denmark',
 'FI': 'Finland',
 'FR': 'France',
 'DE': 'Germany',
 'IE': 'Ireland',
 'IT': 'Italy',
 'NL': 'Netherlands',
 'NO': 'Norway',
 'PT': 'Portugal',
 'ES': 'Spain',
 'SE': 'Sweden',
 'GB': 'Great Britain',
 'GB-NIR': 'Northern Ireland',
 # Others
 'BE': 'Belgium',
 'AT': 'Austria'
}

GEN_TYPES = ["Solar", "Wind Onshore", "Wind Offshore"]

MIN_TIMESTEP = '15 min'
