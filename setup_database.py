#!/usr/bin/env python3
"""
Database Setup Script for Blog Analysis Pipeline
This script helps you set up the PostgreSQL database and user for the blog analysis pipeline.

Run this script BEFORE running the main pipeline script.
"""

import psycopg2
import sys

def create_database_and_user():
    """
    Create the database and user for the blog analysis pipeline.
    """
    print("🚀 Setting up PostgreSQL database for Blog Analysis Pipeline...")
    print("=" * 60)
    
    # Default connection parameters (modify as needed)
    default_config = {
        'host': 'localhost',
        'user': 'postgres',
        'password': '',  # You'll be prompted for this
        'port': '5432'
    }
    
    # Get password from user
    if not default_config['password']:
        default_config['password'] = input("Enter your PostgreSQL password: ")
    
    try:
        # Connect to PostgreSQL server
        print("📡 Connecting to PostgreSQL server...")
        conn = psycopg2.connect(**default_config)
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Create database
        print("🗄️  Creating database 'blog_analysis'...")
        try:
            cursor.execute("CREATE DATABASE blog_analysis")
            print("✅ Database 'blog_analysis' created successfully!")
        except psycopg2.errors.DuplicateDatabase:
            print("ℹ️  Database 'blog_analysis' already exists.")
        
        # Create user (optional)
        create_user = input("\nWould you like to create a dedicated user for this project? (y/n): ").lower().strip()
        
        if create_user == 'y':
            username = input("Enter username for the new user (default: blog_user): ").strip()
            if not username:
                username = 'blog_user'
            
            password = input(f"Enter password for user '{username}': ")
            
            try:
                cursor.execute(f"CREATE USER {username} WITH PASSWORD %s", (password,))
                cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE blog_analysis TO {username}")
                print(f"✅ User '{username}' created successfully with full privileges!")
                
                # Show the configuration to use
                print("\n📋 Use this configuration in your blog_analysis_pipeline.py:")
                print(f"db_config = {{")
                print(f"    'host': '{default_config['host']}',")
                print(f"    'database': 'blog_analysis',")
                print(f"    'user': '{username}',")
                print(f"    'password': '{password}',")
                print(f"    'port': '{default_config['port']}'")
                print(f"}}")
                
            except psycopg2.errors.DuplicateObject:
                print(f"ℹ️  User '{username}' already exists.")
                cursor.execute(f"GRANT ALL PRIVILEGES ON DATABASE blog_analysis TO {username}")
                print(f"✅ Privileges granted to existing user '{username}'!")
        else:
            print("\n📋 Use this configuration in your blog_analysis_pipeline.py:")
            print(f"db_config = {{")
            print(f"    'host': '{default_config['host']}',")
            print(f"    'database': 'blog_analysis',")
            print(f"    'user': '{default_config['user']}',")
            print(f"    'password': '{default_config['password']}',")
            print(f"    'port': '{default_config['port']}'")
            print(f"}}")
        
        print("\n🎉 Database setup completed successfully!")
        print("You can now run the main pipeline script: python blog_analysis_pipeline.py")
        
    except psycopg2.OperationalError as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        print("\n🔧 Troubleshooting tips:")
        print("1. Make sure PostgreSQL is installed and running")
        print("2. Check if the service is started")
        print("3. Verify your connection parameters")
        print("4. Try connecting with pgAdmin or psql first")
        return False
        
    except Exception as e:
        print(f"❌ An error occurred: {e}")
        return False
        
    finally:
        if 'conn' in locals():
            conn.close()
    
    return True

def main():
    """
    Main function to run the database setup.
    """
    print("Blog Analysis Pipeline - Database Setup")
    print("=" * 40)
    
    # Check if psycopg2 is available
    try:
        import psycopg2
    except ImportError:
        print("❌ psycopg2 is not installed.")
        print("Please install it first: pip install psycopg2-binary")
        return
    
    # Run setup
    if create_database_and_user():
        print("\n✅ Setup completed successfully!")
    else:
        print("\n❌ Setup failed. Please check the error messages above.")
        sys.exit(1)

if __name__ == "__main__":
    main()
