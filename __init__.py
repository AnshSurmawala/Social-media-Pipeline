"""
Social Media Data Pipeline

A Python-based producer-consumer pipeline for processing social media data.
"""

__version__ = "1.0.0"
__author__ = "Social Media Pipeline Team"

# Import main components for easy access
from .producer import SocialMediaProducer
from .consumer import SocialMediaConsumer

__all__ = ['SocialMediaProducer', 'SocialMediaConsumer']
