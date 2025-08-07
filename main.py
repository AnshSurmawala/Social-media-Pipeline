"""
Social Media Data Pipeline Main Application

Orchestrates the producer-consumer pipeline for social media data processing.
"""

import json
import logging
import queue
import signal
import sys
import time
import threading
from datetime import datetime

from src.producer import SocialMediaProducer, run_producer_thread
from src.consumer import SocialMediaConsumer, run_consumer_thread


class PipelineOrchestrator:
    """
    Orchestrates the social media data pipeline with producer and consumer components.
    """
    
    def __init__(self, config: dict = None):
        """
        Initialize the pipeline orchestrator.
        
        Args:
            config: Configuration dictionary for pipeline parameters
        """
        # Default configuration
        default_config = {
            'max_posts': 100,
            'production_interval': 0.5,
            'queue_max_size': 50,
            'consumer_timeout': 1.0,
            'log_level': 'INFO',
            'export_results': True,
            'export_filepath': 'pipeline_results.json'
        }
        
        self.config = {**default_config, **(config or {})}
        self.setup_logging()
        
        # Initialize components
        self.data_queue = queue.Queue(maxsize=self.config['queue_max_size'])
        self.producer = SocialMediaProducer(
            data_queue=self.data_queue,
            max_posts=self.config['max_posts']
        )
        self.consumer = SocialMediaConsumer(data_queue=self.data_queue)
        
        # Thread management
        self.producer_thread = None
        self.consumer_thread = None
        self.is_running = False
        
        # Setup signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self.signal_handler)
        signal.signal(signal.SIGTERM, self.signal_handler)
        
        self.logger = logging.getLogger(__name__)
    
    def setup_logging(self):
        """Configure logging for the pipeline."""
        log_level = getattr(logging, self.config['log_level'].upper())
        
        logging.basicConfig(
            level=log_level,
            format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
            handlers=[
                logging.StreamHandler(sys.stdout),
                logging.FileHandler('pipeline.log')
            ]
        )
        
        # Reduce queue module logging noise
        logging.getLogger('queue').setLevel(logging.WARNING)
    
    def signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully."""
        self.logger.info(f"Received signal {signum}, initiating graceful shutdown...")
        self.stop_pipeline()
    
    def start_pipeline(self):
        """Start the complete data pipeline."""
        self.logger.info("Starting Social Media Data Pipeline")
        self.logger.info(f"Configuration: {self.config}")
        
        try:
            self.is_running = True
            
            # Start consumer first to be ready for data
            self.logger.info("Starting consumer...")
            self.consumer_thread = run_consumer_thread(
                self.consumer,
                timeout=self.config['consumer_timeout']
            )
            
            # Brief delay to ensure consumer is ready
            time.sleep(0.1)
            
            # Start producer
            self.logger.info("Starting producer...")
            self.producer_thread = run_producer_thread(
                self.producer,
                interval=self.config['production_interval']
            )
            
            # Monitor pipeline execution
            self.monitor_pipeline()
            
        except Exception as e:
            self.logger.error(f"Error starting pipeline: {e}")
            self.stop_pipeline()
            raise
    
    def monitor_pipeline(self):
        """Monitor the pipeline execution and provide periodic updates."""
        start_time = time.time()
        last_stats_time = start_time
        
        try:
            while self.is_running:
                current_time = time.time()
                
                # Check if producer thread is still alive
                if self.producer_thread and not self.producer_thread.is_alive():
                    if self.producer.posts_generated >= self.config['max_posts']:
                        self.logger.info("Producer completed successfully")
                        break
                    else:
                        self.logger.error("Producer thread died unexpectedly")
                        break
                
                # Print stats every 10 seconds
                if current_time - last_stats_time >= 10:
                    self.print_pipeline_stats()
                    last_stats_time = current_time
                
                time.sleep(1)
            
            # Wait for remaining items to be processed
            self.logger.info("Waiting for queue to empty...")
            self.data_queue.join()
            
            # Generate final results
            self.generate_final_results()
            
        except KeyboardInterrupt:
            self.logger.info("Pipeline interrupted by user")
        except Exception as e:
            self.logger.error(f"Error in pipeline monitoring: {e}")
        finally:
            self.stop_pipeline()
    
    def print_pipeline_stats(self):
        """Print current pipeline statistics."""
        producer_stats = self.producer.get_stats()
        consumer_stats = self.consumer.get_stats()
        
        self.logger.info("=== Pipeline Status ===")
        self.logger.info(f"Producer: {producer_stats['posts_generated']}/{producer_stats['max_posts']} posts generated")
        self.logger.info(f"Consumer: {consumer_stats['processed_posts']} processed, {consumer_stats['failed_posts']} failed")
        self.logger.info(f"Queue size: {consumer_stats['queue_size']}")
        self.logger.info(f"Success rate: {consumer_stats['success_rate']}%")
        self.logger.info("=====================")
    
    def stop_pipeline(self):
        """Stop all pipeline components gracefully."""
        self.is_running = False
        
        # Stop producer
        if self.producer:
            self.producer.stop_production()
        
        # Stop consumer
        if self.consumer:
            self.consumer.stop_consumption()
        
        # Wait for threads to complete
        if self.producer_thread and self.producer_thread.is_alive():
            self.logger.info("Waiting for producer thread to complete...")
            self.producer_thread.join(timeout=5)
        
        if self.consumer_thread and self.consumer_thread.is_alive():
            self.logger.info("Waiting for consumer thread to complete...")
            self.consumer_thread.join(timeout=5)
        
        self.logger.info("Pipeline stopped")
    
    def generate_final_results(self):
        """Generate and display final pipeline results."""
        self.logger.info("Generating final results...")
        
        # Get comprehensive analytics
        analytics = self.consumer.get_analytics()
        
        # Print summary
        self.logger.info("=== FINAL RESULTS ===")
        summary = analytics.get('summary', {})
        self.logger.info(f"Total posts processed: {summary.get('total_processed', 0)}")
        self.logger.info(f"Total posts failed: {summary.get('total_failed', 0)}")
        self.logger.info(f"Success rate: {summary.get('success_rate', 0)}%")
        self.logger.info(f"Average processing time: {summary.get('avg_processing_time_ms', 0)}ms")
        
        # Platform distribution
        platform_dist = analytics.get('platform_distribution', {})
        if platform_dist:
            self.logger.info(f"Platform distribution: {platform_dist}")
        
        # Top posts
        top_posts = analytics.get('top_performing_posts', [])
        if top_posts:
            self.logger.info("Top performing posts:")
            for i, post in enumerate(top_posts[:3], 1):
                self.logger.info(f"  {i}. {post['post_id']} (Score: {post['popularity_score']})")
        
        # Export results if configured
        if self.config['export_results']:
            export_success = self.consumer.export_data(
                self.config['export_filepath'],
                format='json'
            )
            if export_success:
                self.logger.info(f"Results exported to {self.config['export_filepath']}")
        
        self.logger.info("====================")


def main():
    """Main application entry point."""
    # Configuration for the pipeline
    pipeline_config = {
        'max_posts': 50,           # Number of posts to generate
        'production_interval': 0.8, # Seconds between post generation
        'queue_max_size': 20,       # Maximum queue size
        'consumer_timeout': 1.0,    # Consumer queue timeout
        'log_level': 'INFO',        # Logging level
        'export_results': True,     # Export results to file
        'export_filepath': 'social_media_pipeline_results.json'
    }
    
    # Create and start pipeline
    orchestrator = PipelineOrchestrator(config=pipeline_config)
    
    try:
        orchestrator.start_pipeline()
    except Exception as e:
        logging.error(f"Pipeline failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
