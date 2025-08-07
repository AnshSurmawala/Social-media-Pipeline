"""
Social Media Data Producer

Generates simulated social media posts and feeds them into the processing queue.
"""

import json
import logging
import queue
import random
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional


class SocialMediaProducer:
    """
    Produces simulated social media data for the pipeline.
    
    This class generates realistic social media posts with various attributes
    including user information, content, engagement metrics, and timestamps.
    """
    
    def __init__(self, data_queue: queue.Queue, max_posts: int = 1000):
        """
        Initialize the social media producer.
        
        Args:
            data_queue: Queue to send generated posts to
            max_posts: Maximum number of posts to generate
        """
        self.data_queue = data_queue
        self.max_posts = max_posts
        self.posts_generated = 0
        self.is_running = False
        self.logger = logging.getLogger(__name__)
        
        # Sample data for realistic post generation
        self.platforms = ['twitter', 'facebook', 'instagram', 'linkedin', 'tiktok']
        self.usernames = [
            'tech_enthusiast', 'data_scientist', 'social_guru', 'content_creator',
            'digital_nomad', 'startup_founder', 'marketing_pro', 'developer_life',
            'business_insider', 'creative_mind', 'innovation_hub', 'trend_setter'
        ]
        self.content_templates = [
            "Just discovered an amazing new tool for {topic}! #tech #innovation",
            "Thoughts on the latest trends in {topic}? Share your insights below!",
            "Breaking: Major breakthrough in {topic} industry announced today",
            "Tutorial: How to master {topic} in 5 simple steps",
            "Weekly roundup: Top {topic} news you shouldn't miss",
            "Behind the scenes: Our journey with {topic} development",
            "Quick tip: Boost your {topic} productivity with this simple hack",
            "Community question: What's your favorite {topic} resource?"
        ]
        self.topics = [
            'artificial intelligence', 'machine learning', 'data science',
            'cloud computing', 'cybersecurity', 'blockchain', 'mobile development',
            'web development', 'digital marketing', 'social media', 'entrepreneurship'
        ]
    
    def generate_post(self) -> Dict:
        """
        Generate a single social media post with realistic data.
        
        Returns:
            Dictionary containing post data
        """
        post_id = f"post_{self.posts_generated + 1}_{int(time.time())}"
        user_id = random.randint(1000, 9999)
        username = random.choice(self.usernames)
        platform = random.choice(self.platforms)
        topic = random.choice(self.topics)
        content = random.choice(self.content_templates).format(topic=topic)
        
        # Generate realistic engagement metrics based on platform
        engagement_multiplier = {
            'twitter': 1.0,
            'facebook': 1.5,
            'instagram': 2.0,
            'linkedin': 0.8,
            'tiktok': 3.0
        }
        
        base_engagement = random.randint(10, 1000)
        multiplier = engagement_multiplier.get(platform, 1.0)
        
        # Generate timestamp within the last 24 hours
        now = datetime.now()
        time_offset = timedelta(hours=random.uniform(0, 24))
        post_time = now - time_offset
        
        post_data = {
            'post_id': post_id,
            'user_id': user_id,
            'username': username,
            'platform': platform,
            'content': content,
            'topic': topic,
            'engagement': {
                'likes': int(base_engagement * multiplier * random.uniform(0.8, 1.2)),
                'shares': int(base_engagement * multiplier * 0.1 * random.uniform(0.5, 1.5)),
                'comments': int(base_engagement * multiplier * 0.05 * random.uniform(0.3, 2.0)),
                'views': int(base_engagement * multiplier * 10 * random.uniform(5, 15))
            },
            'metadata': {
                'timestamp': post_time.isoformat(),
                'language': 'en',
                'verified_user': random.choice([True, False]),
                'has_media': random.choice([True, False]),
                'hashtags': self._generate_hashtags(topic),
                'mentions': random.randint(0, 3)
            },
            'sentiment': random.choice(['positive', 'neutral', 'negative']),
            'category': self._categorize_content(content, topic)
        }
        
        return post_data
    
    def _generate_hashtags(self, topic: str) -> List[str]:
        """Generate relevant hashtags for a post based on topic."""
        hashtag_pools = {
            'artificial intelligence': ['#AI', '#MachineLearning', '#Tech', '#Innovation'],
            'machine learning': ['#ML', '#DataScience', '#AI', '#Analytics'],
            'data science': ['#DataScience', '#BigData', '#Analytics', '#ML'],
            'cloud computing': ['#Cloud', '#AWS', '#Azure', '#Tech'],
            'cybersecurity': ['#CyberSecurity', '#InfoSec', '#Security', '#Tech'],
            'blockchain': ['#Blockchain', '#Crypto', '#Web3', '#Innovation'],
            'mobile development': ['#MobileDev', '#iOS', '#Android', '#Tech'],
            'web development': ['#WebDev', '#JavaScript', '#Frontend', '#Backend'],
            'digital marketing': ['#DigitalMarketing', '#Marketing', '#SocialMedia', '#Growth'],
            'social media': ['#SocialMedia', '#Marketing', '#Content', '#Engagement'],
            'entrepreneurship': ['#Startup', '#Entrepreneur', '#Business', '#Innovation']
        }
        
        relevant_hashtags = hashtag_pools.get(topic, ['#Tech', '#Innovation'])
        return random.sample(relevant_hashtags, k=random.randint(1, 3))
    
    def _categorize_content(self, content: str, topic: str) -> str:
        """Categorize content based on keywords and topic."""
        if 'tutorial' in content.lower() or 'how to' in content.lower():
            return 'educational'
        elif 'breaking' in content.lower() or 'news' in content.lower():
            return 'news'
        elif 'question' in content.lower() or '?' in content:
            return 'discussion'
        elif 'tip' in content.lower() or 'hack' in content.lower():
            return 'advice'
        else:
            return 'general'
    
    def start_production(self, interval: float = 1.0):
        """
        Start producing social media posts at regular intervals.
        
        Args:
            interval: Time interval between posts in seconds
        """
        self.is_running = True
        self.logger.info(f"Starting social media data production with {interval}s interval")
        
        try:
            while self.is_running and self.posts_generated < self.max_posts:
                post_data = self.generate_post()
                
                try:
                    # Add post to queue with timeout to prevent blocking
                    self.data_queue.put(post_data, timeout=5.0)
                    self.posts_generated += 1
                    
                    self.logger.debug(f"Generated post {self.posts_generated}: {post_data['post_id']}")
                    
                    # Log progress every 10 posts
                    if self.posts_generated % 10 == 0:
                        self.logger.info(f"Generated {self.posts_generated} posts")
                    
                except queue.Full:
                    self.logger.warning("Queue is full, waiting before retry...")
                    time.sleep(interval * 2)
                    continue
                
                time.sleep(interval)
                
        except Exception as e:
            self.logger.error(f"Error in data production: {e}")
            raise
        finally:
            self.is_running = False
            self.logger.info(f"Production completed. Total posts generated: {self.posts_generated}")
    
    def stop_production(self):
        """Stop the data production process."""
        self.is_running = False
        self.logger.info("Stopping data production...")
    
    def get_stats(self) -> Dict:
        """
        Get production statistics.
        
        Returns:
            Dictionary with production stats
        """
        return {
            'posts_generated': self.posts_generated,
            'max_posts': self.max_posts,
            'is_running': self.is_running,
            'completion_percentage': (self.posts_generated / self.max_posts) * 100
        }


def run_producer_thread(producer: SocialMediaProducer, interval: float = 1.0):
    """
    Run the producer in a separate thread.
    
    Args:
        producer: SocialMediaProducer instance
        interval: Production interval in seconds
    """
    thread = threading.Thread(
        target=producer.start_production,
        args=(interval,),
        name="SocialMediaProducer"
    )
    thread.daemon = True
    thread.start()
    return thread
