# Blog Content Analysis Pipeline

A comprehensive data pipeline for analyzing blog content from multiple companies. This system automatically scrapes blog posts, performs AI-powered text analysis, and stores enriched data in a PostgreSQL database for further analysis.

## 🚀 Features

- **Automated Database Setup**: Creates PostgreSQL tables with proper schemas and relationships
- **CSV Data Ingestion**: Loads company and performance data from CSV files
- **Intelligent Web Scraping**: Automatically discovers and scrapes blog posts from company URLs
- **AI-Powered Text Analysis**: 
  - Readability scoring (Flesch-Kincaid)
  - Tone analysis (6 categories: Informative, Inspirational, Authoritative, Persuasive, Humorous, Empathetic)
  - Word frequency analysis
  - Reading time estimation
  - Content complexity classification
- **Robust Error Handling**: Comprehensive logging and graceful failure recovery
- **Respectful Scraping**: Built-in delays and proper user agents

## 📋 Prerequisites

### System Requirements
- Python 3.8 or higher
- PostgreSQL 12 or higher
- Windows 10/11 (tested on Windows 10.0.19045)

### Software Installation
1. **Python**: Download and install from [python.org](https://python.org)
2. **PostgreSQL**: Download and install from [postgresql.org](https://postgresql.org)
3. **Git** (optional): For version control

## 🛠️ Installation

### 1. Clone or Download the Project
```bash
# If using Git
git clone <repository-url>
cd blog_analysis_pipeline

# Or simply download and extract the files to a folder
```

### 2. Install Python Dependencies
```bash
pip install -r requirements.txt
```

### 3. PostgreSQL Setup
1. **Install PostgreSQL** if not already installed
2. **Create a new database**:
   ```sql
   CREATE DATABASE blog_analysis;
   ```
3. **Create a user** (optional but recommended):
   ```sql
   CREATE USER blog_user WITH PASSWORD 'your_secure_password';
   GRANT ALL PRIVILEGES ON DATABASE blog_analysis TO blog_user;
   ```

### 4. Configure Database Connection
Edit the `blog_analysis_pipeline.py` file and update the database configuration:

```python
db_config = {
    'host': 'localhost',           # Your PostgreSQL host
    'database': 'blog_analysis',   # Database name
    'user': 'postgres',            # Your username
    'password': 'your_password',   # Your password
    'port': '5432'                 # PostgreSQL port (default: 5432)
}
```

## 📊 Data Structure

### Input Files

#### `companies.csv`
```csv
company_id,company_name,company_url
1,Crispy Content,https://www.crispycontent.de/en/blog
```

#### `performance.csv`
```csv
metrics_id,text_id,views,CTR,CR,reshares
1,1,1500,0.025,0.08,4
2,2,2200,0.032,0.12,4
```

### Database Tables

#### Companies Table
- `company_id` (INT, PRIMARY KEY): Unique identifier for each company
- `company_name` (TEXT): Company name
- `company_url` (TEXT): Main blog URL to scrape

#### Texts Table
- `text_id` (SERIAL, PRIMARY KEY): Auto-incrementing unique identifier
- `company_id` (INT, FOREIGN KEY): References Companies table
- `title` (TEXT): Blog post title
- `publication_date` (DATE): When the post was published
- `category` (TEXT): Content category
- `tags` (TEXT): Associated tags
- `content_text` (TEXT): Full blog post content
- `word_count` (INT): Total word count
- `avg_sentence_length` (DECIMAL): Average words per sentence
- `avg_reading_time` (INT): Estimated reading time in minutes
- `tone_label` (TEXT): AI-detected tone (6 categories)
- `most_frequent_words` (TEXT): Top 10 most frequent words
- `readability_score` (DECIMAL): Flesch-Kincaid readability score
- `optimal_complexity` (TEXT): Complexity classification
- `semantic_similarity_score` (DECIMAL): Placeholder for future enhancement

#### Performance Table
- `metrics_id` (INT, PRIMARY KEY): Unique identifier for metrics
- `text_id` (INT, FOREIGN KEY): References Texts table
- `views` (INT): Number of views
- `CTR` (DECIMAL): Click-through rate
- `CR` (DECIMAL): Conversion rate
- `reshares` (INT): Number of reshares

## 🚀 Usage

### Basic Execution
```bash
python blog_analysis_pipeline.py
```

### What Happens When You Run the Script

1. **Database Connection**: Connects to PostgreSQL using your configuration
2. **Table Creation**: Creates the three tables if they don't exist
3. **CSV Data Loading**: Loads company and performance data from CSV files
4. **Web Scraping**: 
   - Iterates through company URLs
   - Discovers blog post links
   - Scrapes individual post content
5. **Text Analysis**: 
   - Calculates readability scores
   - Determines tone and complexity
   - Extracts key metrics
6. **Data Storage**: Inserts all analyzed data into the database

### Monitoring Progress
The script provides detailed logging:
- Console output shows real-time progress
- `pipeline.log` file contains detailed execution history
- Progress indicators for each major step

## 🔧 Configuration Options

### Scraping Behavior
- **Post Limit**: Currently set to 20 posts per company (modifiable in code)
- **Delay Between Requests**: Random 1-3 second delays to be respectful
- **Timeout**: 10-15 second timeouts for web requests

### Analysis Parameters
- **Reading Speed**: 200 words per minute (industry standard)
- **Complexity Thresholds**:
  - "Too Basic": > 60 readability score
  - "Optimal": 30-60 readability score
  - "Too Complex": < 30 readability score

## 📈 Output and Results

### Database Queries
After running the pipeline, you can analyze your data:

```sql
-- View all scraped blog posts
SELECT c.company_name, t.title, t.readability_score, t.tone_label
FROM Texts t
JOIN Companies c ON t.company_id = c.company_id
ORDER BY t.publication_date DESC;

-- Analyze tone distribution
SELECT tone_label, COUNT(*) as count
FROM Texts
GROUP BY tone_label
ORDER BY count DESC;

-- Find most readable content
SELECT title, readability_score, optimal_complexity
FROM Texts
WHERE readability_score > 50
ORDER BY readability_score DESC;
```

### Log Files
- `pipeline.log`: Detailed execution log with timestamps
- Console output: Real-time progress and status updates

## 🚨 Troubleshooting

### Common Issues

#### Database Connection Errors
```
Failed to connect to database: connection to server failed
```
**Solutions:**
- Verify PostgreSQL is running
- Check host, port, and credentials
- Ensure database exists
- Check firewall settings

#### Missing Dependencies
```
ModuleNotFoundError: No module named 'psycopg2'
```
**Solutions:**
```bash
pip install -r requirements.txt
# Or install individually:
pip install psycopg2-binary pandas requests beautifulsoup4 textstat nltk
```

#### Web Scraping Failures
```
Failed to scrape blog links from [URL]
```
**Solutions:**
- Check if the URL is accessible
- Verify the website allows scraping
- Check internet connection
- Some sites may block automated requests

#### NLTK Data Issues
```
Could not download NLTK data
```
**Solutions:**
```python
import nltk
nltk.download('punkt')
nltk.download('stopwords')
nltk.download('vader_lexicon')
```

### Performance Optimization

#### For Large Datasets
- Increase database connection pool size
- Add database indexes on frequently queried columns
- Implement batch processing for large CSV files

#### For Faster Scraping
- Reduce delay between requests (be respectful!)
- Implement parallel processing (with caution)
- Use proxy rotation for high-volume scraping

## 🔒 Security Considerations

### Database Security
- Use strong passwords for database users
- Limit database user permissions
- Consider using connection pooling for production

### Web Scraping Ethics
- Respect robots.txt files
- Implement reasonable delays between requests
- Use proper user agent strings
- Don't overload servers

### Data Privacy
- Ensure compliance with data protection regulations
- Implement data retention policies
- Secure database access

## 📚 Advanced Usage

### Custom Analysis
You can extend the pipeline by:
- Adding new text analysis metrics
- Implementing custom tone detection algorithms
- Adding sentiment analysis scores
- Creating custom readability formulas

### Integration
The pipeline can be integrated with:
- Business intelligence tools (Tableau, Power BI)
- Data warehouses
- Machine learning pipelines
- Content management systems

### Scheduling
For production use, consider:
- Running as a scheduled task (Windows Task Scheduler)
- Using cron jobs (Linux/Mac)
- Implementing as a service
- Adding monitoring and alerting

## 🤝 Support and Contributing

### Getting Help
- Check the log files for detailed error information
- Review the console output for progress and status
- Ensure all prerequisites are met
- Verify database connectivity

### Reporting Issues
When reporting issues, include:
- Python version
- Operating system
- Error messages from logs
- Steps to reproduce the problem

### Contributing
To contribute improvements:
- Test thoroughly before submitting
- Follow the existing code style
- Add appropriate documentation
- Include error handling for edge cases

## 📄 License

This project is provided as-is for educational and business use. Please ensure compliance with applicable laws and website terms of service when scraping content.

## 🎯 Next Steps

After successfully running the pipeline:

1. **Explore Your Data**: Use the database queries above to understand your content
2. **Create Dashboards**: Connect to BI tools for visualization
3. **Set Up Monitoring**: Implement regular pipeline execution
4. **Extend Functionality**: Add custom analysis features
5. **Scale Up**: Add more companies and content sources

---

**Happy Analyzing! 🚀**

For questions or support, refer to the troubleshooting section above or check the log files for detailed information about your specific execution.
