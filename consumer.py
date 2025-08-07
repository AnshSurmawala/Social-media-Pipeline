"""
Social Media Data Consumer

Processes social media posts from the queue and performs data validation,
transformation, and analysis.
"""

import json
import logging
import queue
import threading
import time
from datetime import datetime
from typing import Dict, List, Optional, Set
from collections import defaultdict, Counter


class SocialMediaConsumer:
    """
    Consumes and processes social media data from the pipeline queue.
    
    This class handles data validation, transformation, analysis, and storage
    of social media posts with comprehensive error handling and monitoring.
    """
    
    def __init__(self, data_queue: queue.Queue):
        """
        Initialize the social media consumer.
        
        Args:
            data_queue: Queue to receive posts from
        """
        self.data_queue = data_queue
        self.is_running = False
        self.processed_posts = 0
        self.failed_posts = 0
        self.logger = logging.getLogger(__name__)
        
        # Data storage and analytics
        self.processed_data = []
        self.platform_stats = defaultdict(int)
        self.topic_stats = defaultdict(int)
        self.engagement_stats = defaultdict(list)
        self.sentiment_stats = Counter()
        self.category_stats = Counter()
        self.hourly_stats = defaultdict(int)
        
        # Validation rules
        self.required_fields = {
            'post_id', 'user_id', 'username', 'platform', 'content',
            'engagement', 'metadata', 'sentiment', 'category'
        }
        self.valid_platforms = {'twitter', 'facebook', 'instagram', 'linkedin', 'tiktok'}
        self.valid_sentiments = {'positive', 'neutral', 'negative'}
        self.valid_categories = {'educational', 'news', 'discussion', 'advice', 'general'}
        
        # Processing metrics
        self.processing_times = []
        self.error_log = []
    
    def validate_post(self, post_data: Dict) -> tuple[bool, List[str]]:
        """
        Validate a social media post for required fields and data integrity.
        
        Args:
            post_data: Post data to validate
            
        Returns:
            Tuple of (is_valid, error_messages)
        """
        errors = []
        
        try:
            # Check for required top-level fields
            missing_fields = self.required_fields - set(post_data.keys())
            if missing_fields:
                errors.append(f"Missing required fields: {missing_fields}")
            
            # Validate post_id
            if 'post_id' in post_data:
                if not isinstance(post_data['post_id'], str) or not post_data['post_id'].strip():
                    errors.append("post_id must be a non-empty string")
            
            # Validate user_id
            if 'user_id' in post_data:
                if not isinstance(post_data['user_id'], int) or post_data['user_id'] <= 0:
                    errors.append("user_id must be a positive integer")
            
            # Validate platform
            if 'platform' in post_data:
                if post_data['platform'] not in self.valid_platforms:
                    errors.append(f"Invalid platform: {post_data['platform']}")
            
            # Validate content
            if 'content' in post_data:
                if not isinstance(post_data['content'], str) or not post_data['content'].strip():
                    errors.append("content must be a non-empty string")
                elif len(post_data['content']) > 10000:  # Reasonable content length limit
                    errors.append("content exceeds maximum length")
            
            # Validate engagement data
            if 'engagement' in post_data:
                engagement = post_data['engagement']
                if not isinstance(engagement, dict):
                    errors.append("engagement must be a dictionary")
                else:
                    required_engagement_fields = {'likes', 'shares', 'comments', 'views'}
                    missing_engagement = required_engagement_fields - set(engagement.keys())
                    if missing_engagement:
                        errors.append(f"Missing engagement fields: {missing_engagement}")
                    
                    # Validate engagement values are non-negative integers
                    for field in required_engagement_fields:
                        if field in engagement:
                            if not isinstance(engagement[field], int) or engagement[field] < 0:
                                errors.append(f"engagement.{field} must be a non-negative integer")
            
            # Validate metadata
            if 'metadata' in post_data:
                metadata = post_data['metadata']
                if not isinstance(metadata, dict):
                    errors.append("metadata must be a dictionary")
                else:
                    # Validate timestamp
                    if 'timestamp' in metadata:
                        try:
                            datetime.fromisoformat(metadata['timestamp'].replace('Z', '+00:00'))
                        except (ValueError, AttributeError):
                            errors.append("metadata.timestamp must be a valid ISO format datetime")
                    
                    # Validate hashtags
                    if 'hashtags' in metadata:
                        if not isinstance(metadata['hashtags'], list):
                            errors.append("metadata.hashtags must be a list")
                        elif not all(isinstance(tag, str) for tag in metadata['hashtags']):
                            errors.append("All hashtags must be strings")
            
            # Validate sentiment
            if 'sentiment' in post_data:
                if post_data['sentiment'] not in self.valid_sentiments:
                    errors.append(f"Invalid sentiment: {post_data['sentiment']}")
            
            # Validate category
            if 'category' in post_data:
                if post_data['category'] not in self.valid_categories:
                    errors.append(f"Invalid category: {post_data['category']}")
            
        except Exception as e:
            errors.append(f"Validation error: {str(e)}")
        
        return len(errors) == 0, errors
    
    def process_post(self, post_data: Dict) -> Optional[Dict]:
        """
        Process a single social media post with validation and transformation.
        
        Args:
            post_data: Raw post data to process
            
        Returns:
            Processed post data or None if processing failed
        """
        start_time = time.time()
        
        try:
            # Validate the post
            is_valid, validation_errors = self.validate_post(post_data)
            if not is_valid:
                self.logger.warning(f"Post validation failed: {validation_errors}")
                self.failed_posts += 1
                self.error_log.append({
                    'post_id': post_data.get('post_id', 'unknown'),
                    'errors': validation_errors,
                    'timestamp': datetime.now().isoformat()
                })
                return None
            
            # Transform and enrich the data
            processed_post = self.transform_post(post_data)
            
            # Update statistics
            self.update_statistics(processed_post)
            
            # Store processed data
            self.processed_data.append(processed_post)
            self.processed_posts += 1
            
            # Record processing time
            processing_time = time.time() - start_time
            self.processing_times.append(processing_time)
            
            self.logger.debug(f"Successfully processed post: {processed_post['post_id']}")
            
            return processed_post
            
        except Exception as e:
            self.logger.error(f"Error processing post: {e}")
            self.failed_posts += 1
            self.error_log.append({
                'post_id': post_data.get('post_id', 'unknown'),
                'errors': [str(e)],
                'timestamp': datetime.now().isoformat()
            })
            return None
    
    def transform_post(self, post_data: Dict) -> Dict:
        """
        Transform and enrich post data with additional computed fields.
        
        Args:
            post_data: Validated post data
            
        Returns:
            Transformed post data
        """
        # Create a copy to avoid modifying original data
        transformed_post = post_data.copy()
        
        # Add processing timestamp
        transformed_post['processed_at'] = datetime.now().isoformat()
        
        # Calculate engagement rate
        engagement = post_data['engagement']
        total_engagement = engagement['likes'] + engagement['shares'] + engagement['comments']
        views = engagement['views']
        engagement_rate = (total_engagement / views * 100) if views > 0 else 0
        transformed_post['engagement_rate'] = round(engagement_rate, 2)
        
        # Calculate content metrics
        content = post_data['content']
        transformed_post['content_metrics'] = {
            'character_count': len(content),
            'word_count': len(content.split()),
            'hashtag_count': len(post_data['metadata'].get('hashtags', [])),
            'mention_count': post_data['metadata'].get('mentions', 0)
        }
        
        # Add popularity score (weighted engagement metric)
        popularity_weights = {
            'likes': 1.0,
            'shares': 3.0,  # Shares are more valuable
            'comments': 2.0,  # Comments show engagement
            'views': 0.1    # Views are common
        }
        
        popularity_score = sum(
            engagement[metric] * weight 
            for metric, weight in popularity_weights.items()
        )
        transformed_post['popularity_score'] = popularity_score
        
        # Classify post size
        char_count = transformed_post['content_metrics']['character_count']
        if char_count < 50:
            post_size = 'short'
        elif char_count < 200:
            post_size = 'medium'
        else:
            post_size = 'long'
        transformed_post['post_size'] = post_size
        
        # Extract hour from timestamp for time-based analysis
        timestamp = datetime.fromisoformat(post_data['metadata']['timestamp'])
        transformed_post['post_hour'] = timestamp.hour
        
        return transformed_post
    
    def update_statistics(self, processed_post: Dict):
        """
        Update running statistics with data from processed post.
        
        Args:
            processed_post: Processed post data
        """
        # Platform statistics
        platform = processed_post['platform']
        self.platform_stats[platform] += 1
        
        # Topic statistics
        topic = processed_post.get('topic', 'unknown')
        self.topic_stats[topic] += 1
        
        # Engagement statistics by platform
        engagement_rate = processed_post['engagement_rate']
        self.engagement_stats[platform].append(engagement_rate)
        
        # Sentiment statistics
        sentiment = processed_post['sentiment']
        self.sentiment_stats[sentiment] += 1
        
        # Category statistics
        category = processed_post['category']
        self.category_stats[category] += 1
        
        # Hourly statistics
        hour = processed_post['post_hour']
        self.hourly_stats[hour] += 1
    
    def start_consumption(self, timeout: float = 1.0):
        """
        Start consuming and processing posts from the queue.
        
        Args:
            timeout: Timeout for queue operations in seconds
        """
        self.is_running = True
        self.logger.info("Starting social media data consumption")
        
        try:
            while self.is_running:
                try:
                    # Get post from queue with timeout
                    post_data = self.data_queue.get(timeout=timeout)
                    
                    if post_data is None:  # Sentinel value to stop
                        break
                    
                    # Process the post
                    processed_post = self.process_post(post_data)
                    
                    # Mark task as done
                    self.data_queue.task_done()
                    
                    # Log progress every 10 posts
                    if self.processed_posts % 10 == 0 and self.processed_posts > 0:
                        self.logger.info(f"Processed {self.processed_posts} posts")
                    
                except queue.Empty:
                    # No data available, continue checking
                    continue
                except Exception as e:
                    self.logger.error(f"Error in consumption loop: {e}")
                    continue
                    
        except KeyboardInterrupt:
            self.logger.info("Consumption interrupted by user")
        except Exception as e:
            self.logger.error(f"Fatal error in data consumption: {e}")
            raise
        finally:
            self.is_running = False
            self.logger.info(f"Consumption completed. Total processed: {self.processed_posts}")
    
    def stop_consumption(self):
        """Stop the data consumption process."""
        self.is_running = False
        self.logger.info("Stopping data consumption...")
    
    def get_analytics(self) -> Dict:
        """
        Generate comprehensive analytics from processed data.
        
        Returns:
            Dictionary containing analytics and insights
        """
        if not self.processed_data:
            return {'message': 'No data processed yet'}
        
        # Calculate average engagement rates by platform
        avg_engagement_by_platform = {}
        for platform, rates in self.engagement_stats.items():
            if rates:
                avg_engagement_by_platform[platform] = round(sum(rates) / len(rates), 2)
        
        # Calculate processing performance metrics
        avg_processing_time = (
            round(sum(self.processing_times) / len(self.processing_times) * 1000, 2)
            if self.processing_times else 0
        )
        
        # Find top performers
        top_posts = sorted(
            self.processed_data,
            key=lambda x: x['popularity_score'],
            reverse=True
        )[:5]
        
        analytics = {
            'summary': {
                'total_processed': self.processed_posts,
                'total_failed': self.failed_posts,
                'success_rate': round((self.processed_posts / (self.processed_posts + self.failed_posts)) * 100, 2),
                'avg_processing_time_ms': avg_processing_time
            },
            'platform_distribution': dict(self.platform_stats),
            'topic_distribution': dict(self.topic_stats),
            'sentiment_distribution': dict(self.sentiment_stats),
            'category_distribution': dict(self.category_stats),
            'engagement_analytics': {
                'avg_engagement_by_platform': avg_engagement_by_platform,
                'hourly_post_distribution': dict(self.hourly_stats)
            },
            'top_performing_posts': [
                {
                    'post_id': post['post_id'],
                    'platform': post['platform'],
                    'popularity_score': post['popularity_score'],
                    'engagement_rate': post['engagement_rate']
                }
                for post in top_posts
            ],
            'error_summary': {
                'total_errors': len(self.error_log),
                'recent_errors': self.error_log[-5:] if self.error_log else []
            }
        }
        
        return analytics
    
    def get_stats(self) -> Dict:
        """
        Get consumption statistics.
        
        Returns:
            Dictionary with consumption stats
        """
        return {
            'processed_posts': self.processed_posts,
            'failed_posts': self.failed_posts,
            'is_running': self.is_running,
            'queue_size': self.data_queue.qsize(),
            'success_rate': (
                round((self.processed_posts / (self.processed_posts + self.failed_posts)) * 100, 2)
                if (self.processed_posts + self.failed_posts) > 0 else 0
            )
        }
    
    def export_data(self, filepath: str, format: str = 'json'):
        """
        Export processed data to file.
        
        Args:
            filepath: Output file path
            format: Export format ('json' or 'csv')
        """
        try:
            if format == 'json':
                with open(filepath, 'w', encoding='utf-8') as f:
                    json.dump({
                        'processed_data': self.processed_data,
                        'analytics': self.get_analytics()
                    }, f, indent=2, ensure_ascii=False)
            else:
                self.logger.warning(f"Unsupported export format: {format}")
                return False
            
            self.logger.info(f"Data exported to {filepath}")
            return True
            
        except Exception as e:
            self.logger.error(f"Failed to export data: {e}")
            return False


def run_consumer_thread(consumer: SocialMediaConsumer, timeout: float = 1.0):
    """
    Run the consumer in a separate thread.
    
    Args:
        consumer: SocialMediaConsumer instance
        timeout: Queue timeout in seconds
    """
    thread = threading.Thread(
        target=consumer.start_consumption,
        args=(timeout,),
        name="SocialMediaConsumer"
    )
    thread.daemon = True
    thread.start()
    return thread
