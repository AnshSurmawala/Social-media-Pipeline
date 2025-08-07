"""
Social Media Pipeline Results Visualizer

Converts JSON analytics data into visual charts and graphs.
"""

import json
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from datetime import datetime
import numpy as np

def load_results(filepath):
    """Load results from JSON file."""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Error: Could not find file {filepath}")
        return None
    except json.JSONDecodeError:
        print(f"Error: Invalid JSON in file {filepath}")
        return None

def create_platform_distribution_chart(analytics, ax):
    """Create pie chart for platform distribution."""
    platform_data = analytics.get('platform_distribution', {})
    if not platform_data:
        ax.text(0.5, 0.5, 'No platform data available', ha='center', va='center')
        return
    
    platforms = list(platform_data.keys())
    counts = list(platform_data.values())
    colors = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FFEAA7']
    
    wedges, texts, autotexts = ax.pie(counts, labels=platforms, autopct='%1.1f%%', 
                                      colors=colors[:len(platforms)], startangle=90)
    ax.set_title('Posts Distribution by Platform', fontsize=14, fontweight='bold', pad=20)
    
    # Make percentage text bold
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')

def create_topic_distribution_chart(analytics, ax):
    """Create bar chart for topic distribution."""
    topic_data = analytics.get('topic_distribution', {})
    if not topic_data:
        ax.text(0.5, 0.5, 'No topic data available', ha='center', va='center')
        return
    
    topics = list(topic_data.keys())
    counts = list(topic_data.values())
    
    # Truncate long topic names for better display
    display_topics = [topic.replace(' ', '\n') if len(topic) > 15 else topic for topic in topics]
    
    bars = ax.bar(range(len(topics)), counts, color='#74B9FF', alpha=0.8)
    ax.set_xlabel('Topics', fontweight='bold')
    ax.set_ylabel('Number of Posts', fontweight='bold')
    ax.set_title('Posts Distribution by Topic', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(range(len(topics)))
    ax.set_xticklabels(display_topics, rotation=45, ha='right', fontsize=9)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{count}', ha='center', va='bottom', fontweight='bold')

def create_sentiment_distribution_chart(analytics, ax):
    """Create pie chart for sentiment distribution."""
    sentiment_data = analytics.get('sentiment_distribution', {})
    if not sentiment_data:
        ax.text(0.5, 0.5, 'No sentiment data available', ha='center', va='center')
        return
    
    sentiments = list(sentiment_data.keys())
    counts = list(sentiment_data.values())
    colors = {'positive': '#00B894', 'neutral': '#FDCB6E', 'negative': '#E17055'}
    chart_colors = [colors.get(sentiment, '#DDD') for sentiment in sentiments]
    
    wedges, texts, autotexts = ax.pie(counts, labels=[s.title() for s in sentiments], 
                                      autopct='%1.1f%%', colors=chart_colors, startangle=90)
    ax.set_title('Sentiment Distribution', fontsize=14, fontweight='bold', pad=20)
    
    for autotext in autotexts:
        autotext.set_color('white')
        autotext.set_fontweight('bold')

def create_category_distribution_chart(analytics, ax):
    """Create bar chart for category distribution."""
    category_data = analytics.get('category_distribution', {})
    if not category_data:
        ax.text(0.5, 0.5, 'No category data available', ha='center', va='center')
        return
    
    categories = list(category_data.keys())
    counts = list(category_data.values())
    
    bars = ax.bar(categories, counts, color='#6C5CE7', alpha=0.8)
    ax.set_xlabel('Categories', fontweight='bold')
    ax.set_ylabel('Number of Posts', fontweight='bold')
    ax.set_title('Posts Distribution by Category', fontsize=14, fontweight='bold', pad=20)
    ax.tick_params(axis='x', rotation=45)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, count in zip(bars, counts):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                f'{count}', ha='center', va='bottom', fontweight='bold')

def create_engagement_by_platform_chart(analytics, ax):
    """Create bar chart for average engagement by platform."""
    engagement_data = analytics.get('engagement_analytics', {}).get('avg_engagement_by_platform', {})
    if not engagement_data:
        ax.text(0.5, 0.5, 'No engagement data available', ha='center', va='center')
        return
    
    platforms = list(engagement_data.keys())
    engagement_rates = list(engagement_data.values())
    
    bars = ax.bar(platforms, engagement_rates, color='#FD79A8', alpha=0.8)
    ax.set_xlabel('Platform', fontweight='bold')
    ax.set_ylabel('Average Engagement Rate (%)', fontweight='bold')
    ax.set_title('Average Engagement Rate by Platform', fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='y', alpha=0.3)
    
    # Add value labels on bars
    for bar, rate in zip(bars, engagement_rates):
        height = bar.get_height()
        ax.text(bar.get_x() + bar.get_width()/2., height + 0.02,
                f'{rate:.2f}%', ha='center', va='bottom', fontweight='bold')

def create_hourly_distribution_chart(analytics, ax):
    """Create line chart for hourly post distribution."""
    hourly_data = analytics.get('engagement_analytics', {}).get('hourly_post_distribution', {})
    if not hourly_data:
        ax.text(0.5, 0.5, 'No hourly data available', ha='center', va='center')
        return
    
    # Convert string hours to integers and sort
    hours = [int(hour) for hour in hourly_data.keys()]
    counts = [hourly_data[str(hour)] for hour in hours]
    
    # Create full 24-hour range
    full_hours = list(range(24))
    full_counts = [hourly_data.get(str(hour), 0) for hour in full_hours]
    
    ax.plot(full_hours, full_counts, marker='o', linewidth=2, markersize=6, color='#00B894')
    ax.fill_between(full_hours, full_counts, alpha=0.3, color='#00B894')
    ax.set_xlabel('Hour of Day', fontweight='bold')
    ax.set_ylabel('Number of Posts', fontweight='bold')
    ax.set_title('Post Distribution by Hour of Day', fontsize=14, fontweight='bold', pad=20)
    ax.set_xticks(range(0, 24, 2))
    ax.grid(True, alpha=0.3)
    ax.set_xlim(0, 23)

def create_top_posts_chart(analytics, ax):
    """Create horizontal bar chart for top performing posts."""
    top_posts = analytics.get('top_performing_posts', [])
    if not top_posts:
        ax.text(0.5, 0.5, 'No top posts data available', ha='center', va='center')
        return
    
    # Take top 5 posts
    top_5_posts = top_posts[:5]
    post_labels = [f"{post['platform'].title()}\n{post['post_id'][-8:]}" for post in top_5_posts]
    scores = [post['popularity_score'] for post in top_5_posts]
    
    colors = ['#E17055', '#74B9FF', '#00B894', '#FDCB6E', '#6C5CE7']
    bars = ax.barh(range(len(post_labels)), scores, color=colors[:len(post_labels)])
    
    ax.set_yticks(range(len(post_labels)))
    ax.set_yticklabels(post_labels)
    ax.set_xlabel('Popularity Score', fontweight='bold')
    ax.set_title('Top 5 Performing Posts', fontsize=14, fontweight='bold', pad=20)
    ax.grid(axis='x', alpha=0.3)
    
    # Add value labels on bars
    for bar, score in zip(bars, scores):
        width = bar.get_width()
        ax.text(width + max(scores) * 0.01, bar.get_y() + bar.get_height()/2.,
                f'{score:,.0f}', ha='left', va='center', fontweight='bold')

def create_summary_info(analytics, ax):
    """Create text summary of key metrics."""
    ax.axis('off')
    
    summary = analytics.get('summary', {})
    
    summary_text = f"""
SOCIAL MEDIA PIPELINE RESULTS SUMMARY

📊 Processing Statistics:
   • Total Posts Processed: {summary.get('total_processed', 'N/A')}
   • Success Rate: {summary.get('success_rate', 'N/A')}%
   • Average Processing Time: {summary.get('avg_processing_time_ms', 'N/A')}ms

🎯 Performance Highlights:
   • Zero Failed Posts: {summary.get('total_failed', 'N/A')} errors
   • Multiple Platforms: {len(analytics.get('platform_distribution', {}))} platforms covered
   • Content Diversity: {len(analytics.get('topic_distribution', {}))} topics analyzed

⭐ Top Platform: {max(analytics.get('platform_distribution', {}), key=analytics.get('platform_distribution', {}).get) if analytics.get('platform_distribution') else 'N/A'}
🔥 Most Engaging: {max(analytics.get('engagement_analytics', {}).get('avg_engagement_by_platform', {}), key=analytics.get('engagement_analytics', {}).get('avg_engagement_by_platform', {}).get) if analytics.get('engagement_analytics', {}).get('avg_engagement_by_platform') else 'N/A'}
📈 Peak Hour: {max(analytics.get('engagement_analytics', {}).get('hourly_post_distribution', {}), key=analytics.get('engagement_analytics', {}).get('hourly_post_distribution', {}).get) if analytics.get('engagement_analytics', {}).get('hourly_post_distribution') else 'N/A'}:00
    """
    
    ax.text(0.05, 0.95, summary_text, transform=ax.transAxes, fontsize=11,
            verticalalignment='top', fontfamily='monospace',
            bbox=dict(boxstyle="round,pad=0.5", facecolor='lightblue', alpha=0.1))

def visualize_pipeline_results(json_filepath='social_media_pipeline_results.json', save_path='pipeline_visualization.png'):
    """Create comprehensive visualization of pipeline results."""
    
    # Load data
    data = load_results(json_filepath)
    if not data:
        return False
    
    analytics = data.get('analytics', {})
    
    # Create figure with subplots
    fig = plt.figure(figsize=(20, 16))
    fig.suptitle('Social Media Data Pipeline - Visual Analytics Dashboard', 
                 fontsize=20, fontweight='bold', y=0.98)
    
    # Create a 3x3 grid of subplots
    ax1 = plt.subplot(3, 3, 1)  # Platform distribution
    ax2 = plt.subplot(3, 3, 2)  # Topic distribution  
    ax3 = plt.subplot(3, 3, 3)  # Sentiment distribution
    ax4 = plt.subplot(3, 3, 4)  # Category distribution
    ax5 = plt.subplot(3, 3, 5)  # Engagement by platform
    ax6 = plt.subplot(3, 3, 6)  # Hourly distribution
    ax7 = plt.subplot(3, 3, 7)  # Top posts
    ax8 = plt.subplot(3, 3, (8, 9))  # Summary info (spans 2 cells)
    
    # Create all charts
    create_platform_distribution_chart(analytics, ax1)
    create_topic_distribution_chart(analytics, ax2) 
    create_sentiment_distribution_chart(analytics, ax3)
    create_category_distribution_chart(analytics, ax4)
    create_engagement_by_platform_chart(analytics, ax5)
    create_hourly_distribution_chart(analytics, ax6)
    create_top_posts_chart(analytics, ax7)
    create_summary_info(analytics, ax8)
    
    # Adjust layout
    plt.tight_layout(rect=[0, 0.03, 1, 0.96])
    
    # Add timestamp
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    fig.text(0.99, 0.01, f'Generated: {timestamp}', ha='right', va='bottom', 
             fontsize=8, style='italic', alpha=0.7)
    
    # Save the visualization
    plt.savefig(save_path, dpi=300, bbox_inches='tight', facecolor='white')
    print(f"✅ Visualization saved as: {save_path}")
    
    # Display the plot
    plt.show()
    
    return True

if __name__ == "__main__":
    # Run the visualization
    success = visualize_pipeline_results()
    if success:
        print("🎉 Pipeline results successfully visualized!")
    else:
        print("❌ Failed to create visualization")