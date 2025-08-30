#!/usr/bin/env python3
"""
Blog Content Analysis Pipeline
A comprehensive data pipeline for analyzing blog content from multiple companies.

This script:
1. Sets up a PostgreSQL database with required tables
2. Loads company and performance data from CSV files
3. Scrapes blog content from company URLs
4. Performs AI-powered text analysis and enrichment
5. Stores all data in the database for further analysis

Author: Anton Ostashov + Cursor
Date: 27.08.2025
"""

import os
import sys
import logging
import re
import time
from datetime import datetime, date
from typing import List, Dict, Optional, Tuple
import random

# Data processing and database libraries
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from psycopg2 import sql

# Web scraping libraries
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urlparse

# Text analysis libraries
import textstat
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import sent_tokenize, word_tokenize
from nltk.sentiment import SentimentIntensityAnalyzer

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('pipeline.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

class BlogAnalysisPipeline:
    """
    Main class for the blog content analysis pipeline.
    Handles database operations, web scraping, and text analysis.
    """
    
    def __init__(self, db_config: Dict[str, str]):
        """
        Initialize the pipeline with database configuration.
        
        Args:
            db_config: Dictionary containing database connection parameters
        """
        self.db_config = db_config
        self.connection = None
        self.cursor = None
        
        # Download required NLTK data
        try:
            nltk.download('punkt', quiet=True)
            nltk.download('stopwords', quiet=True)
            nltk.download('vader_lexicon', quiet=True)
        except Exception as e:
            logger.warning(f"Could not download NLTK data: {e}")
        
        # Initialize stop words
        try:
            self.stop_words = set(stopwords.words('english'))
        except:
            self.stop_words = set(['the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for', 'of', 'with', 'by'])
        
        # Initialize sentiment analyzer
        try:
            self.sentiment_analyzer = SentimentIntensityAnalyzer()
        except:
            self.sentiment_analyzer = None
    
    def connect_to_database(self) -> bool:
        """
        Establish connection to PostgreSQL database.
        
        Returns:
            bool: True if connection successful, False otherwise
        """
        try:
            self.connection = psycopg2.connect(**self.db_config)
            self.cursor = self.connection.cursor()
            logger.info("Successfully connected to PostgreSQL database")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to database: {e}")
            return False
    
    def create_tables(self) -> bool:
        """
        Create the required database tables if they don't exist.
        
        Returns:
            bool: True if tables created successfully, False otherwise
        """
        try:
            # Create Companies table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Companies (
                    company_id INT PRIMARY KEY,
                    company_name TEXT NOT NULL,
                    company_url TEXT NOT NULL
                )
            """)
            
            # Create Texts table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Texts (
                    text_id SERIAL PRIMARY KEY,
                    company_id INT NOT NULL,
                    title TEXT,
                    publication_date DATE,
                    category TEXT,
                    tags TEXT,
                    content_text TEXT,
                    word_count INT,
                    avg_sentence_length DECIMAL(5,2),
                    avg_reading_time INT,
                    tone_label TEXT,
                    most_frequent_words TEXT,
                    readability_score DECIMAL(5,2),
                    optimal_complexity TEXT,
                    semantic_similarity_score DECIMAL(5,2),
                    FOREIGN KEY (company_id) REFERENCES Companies(company_id)
                )
            """)
            
            # Create Performance table
            self.cursor.execute("""
                CREATE TABLE IF NOT EXISTS Performance (
                    metrics_id INT PRIMARY KEY,
                    text_id INT NOT NULL,
                    views INT,
                    CTR DECIMAL(5,4),
                    CR DECIMAL(5,4),
                    reshares INT,
                    FOREIGN KEY (text_id) REFERENCES Texts(text_id)
                )
            """)
            
            self.connection.commit()
            logger.info("Database tables created successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to create tables: {e}")
            self.connection.rollback()
            return False
    
    def load_companies_data(self) -> bool:
        """
        Load only companies data from CSV file.
        
        Returns:
            bool: True if data loaded successfully, False otherwise
        """
        try:
            # Load companies data
            companies_df = pd.read_csv('companies.csv')
            for _, row in companies_df.iterrows():
                self.cursor.execute("""
                    INSERT INTO Companies (company_id, company_name, company_url)
                    VALUES (%s, %s, %s)
                    ON CONFLICT (company_id) DO UPDATE SET
                        company_name = EXCLUDED.company_name,
                        company_url = EXCLUDED.company_url
                """, (int(row['company_id']), str(row['company_name']), str(row['company_url'])))
            
            self.connection.commit()
            logger.info("Companies data loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load companies data: {e}")
            self.connection.rollback()
            return False

    def load_performance_data(self) -> bool:
        """
        Load performance data from CSV file (after Texts table has been populated).
        
        Returns:
            bool: True if data loaded successfully, False otherwise
        """
        try:
            # Load performance data
            performance_df = pd.read_csv('performance.csv')
            for _, row in performance_df.iterrows():
                self.cursor.execute("""
                    INSERT INTO Performance (metrics_id, text_id, views, CTR, CR, reshares)
                    VALUES (%s, %s, %s, %s, %s, %s)
                    ON CONFLICT (metrics_id) DO UPDATE SET
                        text_id = EXCLUDED.text_id,
                        views = EXCLUDED.views,
                        CTR = EXCLUDED.CTR,
                        CR = EXCLUDED.CR,
                        reshares = EXCLUDED.reshares
                """, (int(row['metrics_id']), int(row['text_id']), int(row['views']), float(row['CTR']), float(row['CR']), int(row['reshares'])))
            
            self.connection.commit()
            logger.info("Performance data loaded successfully")
            return True
            
        except Exception as e:
            logger.error(f"Failed to load performance data: {e}")
            self.connection.rollback()
            return False
    
    def get_company_urls(self) -> List[Tuple[int, str]]:
        """
        Retrieve all company URLs from the database.
        
        Returns:
            List of tuples containing (company_id, company_url)
        """
        try:
            self.cursor.execute("SELECT company_id, company_url FROM Companies")
            return self.cursor.fetchall()
        except Exception as e:
            logger.error(f"Failed to retrieve company URLs: {e}")
            return []
    
    def scrape_blog_links(self, base_url: str) -> List[str]:
        """
        Scrape blog post links from a company's main blog URL.
        
        Args:
            base_url: The main blog URL to scrape
            
        Returns:
            List of individual blog post URLs
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(base_url, headers=headers, timeout=10)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Common patterns for blog post links
            blog_links = []
            
            # Look for links that might be blog posts
            for link in soup.find_all('a', href=True):
                href = link.get('href')
                if href:
                    # Convert relative URLs to absolute
                    full_url = urljoin(base_url, href)
                    
                    # Common blog post indicators
                    if any(indicator in full_url.lower() for indicator in [
                        '/blog/', '/post/', '/article/', '/news/', '/insights/',
                        'blog', 'post', 'article', 'news', 'insights'
                    ]):
                        blog_links.append(full_url)
            
            # Remove duplicates and limit to reasonable number
            blog_links = list(set(blog_links))[:20]  # Limit to 20 posts per company
            
            logger.info(f"Found {len(blog_links)} potential blog posts at {base_url}")
            return blog_links
            
        except Exception as e:
            logger.warning(f"Failed to scrape blog links from {base_url}: {e}")
            return []
    
    def scrape_blog_content(self, url: str) -> Optional[Dict[str, str]]:
        """
        Scrape content from an individual blog post URL.
        
        Args:
            url: The blog post URL to scrape
            
        Returns:
            Dictionary containing scraped content or None if failed
        """
        try:
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
            }
            
            response = requests.get(url, headers=headers, timeout=15)
            response.raise_for_status()
            
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Extract title
            title = ""
            title_tag = soup.find('title') or soup.find('h1') or soup.find('h2')
            if title_tag:
                title = title_tag.get_text().strip()
            
            # Extract publication date (common patterns)
            publication_date = None
            date_patterns = [
                r'\d{4}-\d{2}-\d{2}',  # YYYY-MM-DD
                r'\d{2}/\d{2}/\d{4}',  # MM/DD/YYYY
                r'\d{1,2}\s+(?:Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{4}'  # DD Month YYYY
            ]
            
            for pattern in date_patterns:
                date_match = re.search(pattern, str(soup))
                if date_match:
                    try:
                        publication_date = datetime.strptime(date_match.group(), '%Y-%m-%d').date()
                        break
                    except:
                        try:
                            publication_date = datetime.strptime(date_match.group(), '%m/%d/%Y').date()
                            break
                        except:
                            continue
            
            # Default to today if no date found
            if not publication_date:
                publication_date = date.today()
            
            # Extract category and tags (common patterns)
            category = "General"
            tags = ""
            
            # Look for category in common locations
            category_selectors = [
                '.category', '.post-category', '.article-category',
                '[class*="category"]', '[class*="tag"]'
            ]
            
            for selector in category_selectors:
                cat_elem = soup.select_one(selector)
                if cat_elem:
                    category = cat_elem.get_text().strip()
                    break
            
            # Extract main content
            content_text = ""
            content_selectors = [
                'article', '.post-content', '.article-content', '.entry-content',
                '.content', 'main', '.main-content'
            ]
            
            for selector in content_selectors:
                content_elem = soup.select_one(selector)
                if content_elem:
                    # Remove script and style elements
                    for script in content_elem(["script", "style"]):
                        script.decompose()
                    
                    content_text = content_elem.get_text(separator=' ', strip=True)
                    if len(content_text) > 100:  # Ensure we have substantial content
                        break
            
            # If no specific content area found, try body
            if len(content_text) < 100:
                body = soup.find('body')
                if body:
                    for script in body(["script", "style"]):
                        script.decompose()
                    content_text = body.get_text(separator=' ', strip=True)
            
            # Clean up content
            content_text = re.sub(r'\s+', ' ', content_text).strip()
            
            if len(content_text) < 50:  # Skip if content is too short
                return None
            
            return {
                'title': title,
                'publication_date': publication_date,
                'category': category,
                'tags': tags,
                'content_text': content_text
            }
            
        except Exception as e:
            logger.warning(f"Failed to scrape content from {url}: {e}")
            return None
    
    def analyze_text_content(self, content_text: str) -> Dict[str, any]:
        """
        Perform comprehensive text analysis on the content.
        
        Args:
            content_text: The text content to analyze
            
        Returns:
            Dictionary containing analysis results
        """
        try:
            # Basic text statistics
            word_count = len(content_text.split())
            sentences = sent_tokenize(content_text)
            avg_sentence_length = len(content_text.split()) / len(sentences) if sentences else 0
            
            # Reading time (average reading speed: 200 words per minute)
            avg_reading_time = max(1, round(word_count / 200))
            
            # Readability score using Flesch-Kincaid
            readability_score = textstat.flesch_reading_ease(content_text)
            
            # Determine optimal complexity
            if readability_score > 60:
                optimal_complexity = "Too Basic"
            elif readability_score >= 30:
                optimal_complexity = "Optimal"
            else:
                optimal_complexity = "Too Complex"
            
            # Tone analysis
            tone_label = self.analyze_tone(content_text)
            
            # Most frequent words (excluding stop words)
            words = word_tokenize(content_text.lower())
            words = [word for word in words if word.isalpha() and word not in self.stop_words]
            
            if words:
                word_freq = {}
                for word in words:
                    word_freq[word] = word_freq.get(word, 0) + 1
                
                # Get top 10 most frequent words
                top_words = sorted(word_freq.items(), key=lambda x: x[1], reverse=True)[:10]
                most_frequent_words = ', '.join([word for word, _ in top_words])
            else:
                most_frequent_words = ""
            
            return {
                'word_count': word_count,
                'avg_sentence_length': round(avg_sentence_length, 2),
                'avg_reading_time': avg_reading_time,
                'readability_score': round(readability_score, 2),
                'optimal_complexity': optimal_complexity,
                'tone_label': tone_label,
                'most_frequent_words': most_frequent_words
            }
            
        except Exception as e:
            logger.error(f"Failed to analyze text content: {e}")
            # Return default values
            return {
                'word_count': 0,
                'avg_sentence_length': 0.0,
                'avg_reading_time': 0,
                'readability_score': 0.0,
                'optimal_complexity': "Informative",
                'tone_label': "Informative",
                'most_frequent_words': ""
            }
    
    def analyze_tone(self, content_text: str) -> str:
        """
        Analyze the tone of the content using sentiment analysis.
        
        Args:
            content_text: The text content to analyze
            
        Returns:
            Tone label as a string
        """
        try:
            if not self.sentiment_analyzer:
                return "Informative"
            
            # Get sentiment scores
            sentiment_scores = self.sentiment_analyzer.polarity_scores(content_text)
            
            # Analyze text characteristics for tone
            text_lower = content_text.lower()
            
            # Check for specific tone indicators
            if any(word in text_lower for word in ['inspire', 'motivate', 'encourage', 'dream', 'vision']):
                return "Inspirational"
            elif any(word in text_lower for word in ['expert', 'authority', 'certified', 'proven', 'research']):
                return "Authoritative"
            elif any(word in text_lower for word in ['convince', 'persuade', 'convince', 'should', 'must']):
                return "Persuasive"
            elif any(word in text_lower for word in ['funny', 'humor', 'joke', 'hilarious', 'amusing']):
                return "Humorous"
            elif any(word in text_lower for word in ['understand', 'empathy', 'feel', 'care', 'support']):
                return "Empathetic"
            else:
                return "Informative"
                
        except Exception as e:
            logger.warning(f"Failed to analyze tone: {e}")
            return "Informative"
    
    def insert_text_data(self, company_id: int, scraped_data: Dict[str, str], 
                        analysis_data: Dict[str, any]) -> bool:
        """
        Insert scraped and analyzed text data into the database.
        
        Args:
            company_id: The ID of the company
            scraped_data: Dictionary containing scraped content
            analysis_data: Dictionary containing analysis results
            
        Returns:
            bool: True if insertion successful, False otherwise
        """
        try:
            self.cursor.execute("""
                INSERT INTO Texts (
                    company_id, title, publication_date, category, tags,
                    content_text, word_count, avg_sentence_length, avg_reading_time,
                    tone_label, most_frequent_words, readability_score,
                    optimal_complexity, semantic_similarity_score
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                company_id,
                scraped_data['title'],
                scraped_data['publication_date'],
                scraped_data['category'],
                scraped_data['tags'],
                scraped_data['content_text'],
                analysis_data['word_count'],
                analysis_data['avg_sentence_length'],
                analysis_data['avg_reading_time'],
                analysis_data['tone_label'],
                analysis_data['most_frequent_words'],
                analysis_data['readability_score'],
                analysis_data['optimal_complexity'],
                0.0  # Placeholder for semantic similarity score
            ))
            
            self.connection.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to insert text data: {e}")
            self.connection.rollback()
            return False
    
    def run_scraping_pipeline(self) -> bool:
        """
        Run the complete web scraping and analysis pipeline.
        
        Returns:
            bool: True if pipeline completed successfully, False otherwise
        """
        try:
            logger.info("Starting web scraping pipeline...")
            
            company_urls = self.get_company_urls()
            if not company_urls:
                logger.error("No company URLs found in database")
                return False
            
            total_posts_scraped = 0
            
            for company_id, company_url in company_urls:
                logger.info(f"Processing company {company_id}: {company_url}")
                
                # Scrape blog post links
                blog_links = self.scrape_blog_links(company_url)
                
                if not blog_links:
                    logger.warning(f"No blog posts found for company {company_id}")
                    continue
                
                # Scrape and analyze each blog post
                for blog_url in blog_links:
                    try:
                        # Add delay to be respectful to servers
                        time.sleep(random.uniform(1, 3))
                        
                        # Scrape content
                        scraped_data = self.scrape_blog_content(blog_url)
                        if not scraped_data:
                            continue
                        
                        # Analyze content
                        analysis_data = self.analyze_text_content(scraped_data['content_text'])
                        
                        # Insert into database
                        if self.insert_text_data(company_id, scraped_data, analysis_data):
                            total_posts_scraped += 1
                            logger.info(f"Successfully processed blog post: {scraped_data['title'][:50]}...")
                        
                    except Exception as e:
                        logger.warning(f"Failed to process blog post {blog_url}: {e}")
                        continue
            
            logger.info(f"Scraping pipeline completed. Total posts processed: {total_posts_scraped}")
            return True
            
        except Exception as e:
            logger.error(f"Scraping pipeline failed: {e}")
            return False
    
    def close_connection(self):
        """Close the database connection."""
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()
        logger.info("Database connection closed")


def main():
    """
    Main function to run the blog analysis pipeline.
    """
    # Database configuration - UPDATE THESE VALUES FOR YOUR SETUP
    db_config = {
        'host': 'localhost',
        'database': 'blog_analysis',
        'user': 'postgres',
        'password': 'mGrOpEr',
        'port': '5432'
    }
    
    # Create pipeline instance
    pipeline = BlogAnalysisPipeline(db_config)
    
    try:
        # Step 1: Connect to database
        if not pipeline.connect_to_database():
            logger.error("Failed to connect to database. Exiting.")
            return
        
        # Step 2: Create tables
        if not pipeline.create_tables():
            logger.error("Failed to create database tables. Exiting.")
            return
        
        # Step 3: Load only companies data (no performance data yet)
        if not pipeline.load_companies_data():
            logger.error("Failed to load companies data. Exiting.")
            return
        
        # Step 4: Run web scraping and analysis pipeline (creates Texts table entries)
        if not pipeline.run_scraping_pipeline():
            logger.error("Web scraping pipeline failed. Exiting.")
            return
        
        # Step 5: Now load performance data (text_id references will exist)
        if not pipeline.load_performance_data():
            logger.error("Failed to load performance data. Exiting.")
            return
        
        logger.info("Blog analysis pipeline completed successfully!")
        
    except KeyboardInterrupt:
        logger.info("Pipeline interrupted by user")
    except Exception as e:
        logger.error(f"Pipeline failed with error: {e}")
    finally:
        pipeline.close_connection()


if __name__ == "__main__":
    main()
