#!/usr/bin/env python3
"""
Supabase Setup Helper

This script helps you set up and test your Supabase database for the lead generation system.
"""

import os
import sys

# Add modules directory to path
sys.path.append(os.path.join(os.path.dirname(__file__), 'modules'))

from supabase_manager import SupabaseManager

def test_supabase_connection():
    """Test Supabase connection and display database info"""
    print("🔧 Supabase Database Setup Helper")
    print("=" * 40)
    
    # Get credentials from environment or user input
    supabase_url = os.getenv('SUPABASE_URL')
    supabase_key = os.getenv('SUPABASE_KEY')
    
    if not supabase_url:
        supabase_url = input("Enter your Supabase Project URL: ").strip()
    
    if not supabase_key:
        supabase_key = input("Enter your Supabase API Key: ").strip()
    
    if not supabase_url or not supabase_key:
        print("❌ Both Supabase URL and API Key are required")
        return False
    
    try:
        # Test connection
        print("\n🧪 Testing Supabase connection...")
        manager = SupabaseManager(supabase_url, supabase_key)
        
        if manager.test_connection():
            print("✅ Supabase connection successful!")
            
            # Display database stats
            print("\n📊 Database Statistics:")
            stats = manager.get_pipeline_stats()
            
            print(f"  • Search URLs: {stats.get('total_searches', 0)}")
            print(f"  • Raw Contacts: {stats.get('total_raw_contacts', 0)}")
            print(f"  • Processed Leads: {stats.get('total_leads', 0)}")
            print(f"  • Converted Leads: {stats.get('total_converted', 0)}")
            
            # Check if tables exist by trying to get search URLs
            search_urls = manager.get_search_urls()
            print(f"  • Database tables appear to be set up correctly")
            
            return True
        else:
            print("❌ Supabase connection failed")
            return False
            
    except Exception as e:
        print(f"❌ Error testing Supabase connection: {e}")
        return False

def create_sample_search_url():
    """Create a sample search URL for testing"""
    try:
        supabase_url = os.getenv('SUPABASE_URL') or input("Supabase URL: ").strip()
        supabase_key = os.getenv('SUPABASE_KEY') or input("Supabase Key: ").strip()
        
        manager = SupabaseManager(supabase_url, supabase_key)
        
        # Create a test search URL
        test_url = "https://www.linkedin.com/search/results/people/?keywords=marketing%20director"
        result = manager.create_search_url(test_url, "Test URL created by setup script")
        
        if result:
            print("✅ Sample search URL created successfully!")
            print(f"   URL: {test_url}")
            print(f"   ID: {result.get('id')}")
        else:
            print("❌ Failed to create sample search URL")
        
    except Exception as e:
        print(f"❌ Error creating sample search URL: {e}")

def clear_database():
    """Clear all data from database (for testing)"""
    try:
        supabase_url = os.getenv('SUPABASE_URL') or input("Supabase URL: ").strip()
        supabase_key = os.getenv('SUPABASE_KEY') or input("Supabase Key: ").strip()
        
        confirm = input("⚠️  This will DELETE ALL DATA from your database. Type 'yes' to confirm: ")
        if confirm.lower() != 'yes':
            print("Operation cancelled")
            return
        
        manager = SupabaseManager(supabase_url, supabase_key)
        
        if manager.clear_all_data():
            print("✅ Database cleared successfully!")
        else:
            print("❌ Failed to clear database")
        
    except Exception as e:
        print(f"❌ Error clearing database: {e}")

def show_schema_instructions():
    """Display database schema setup instructions"""
    print("\n🏗️  Database Schema Setup Instructions:")
    print("=" * 45)
    print()
    print("1. Create a new Supabase project:")
    print("   • Go to https://supabase.com")
    print("   • Click 'New Project'")
    print("   • Choose your organization and project name")
    print()
    print("2. Get your credentials:")
    print("   • Go to Settings → API")
    print("   • Copy your Project URL")
    print("   • Copy your 'anon' public API key")
    print()
    print("3. Set up the database schema:")
    print("   • Go to SQL Editor in your Supabase dashboard")
    print("   • Copy the contents of 'database_schema.sql'")
    print("   • Paste and run the SQL to create tables")
    print()
    print("4. Test the connection:")
    print("   • Use this script or the React UI to test")
    print("   • The UI Database tab has a connection test button")
    print()
    
    schema_file = os.path.join(os.path.dirname(__file__), 'database_schema.sql')
    if os.path.exists(schema_file):
        print(f"📄 Schema file location: {schema_file}")
    else:
        print("❌ Schema file not found. Make sure 'database_schema.sql' exists.")

def main():
    """Main menu"""
    while True:
        print("\n🗄️  Supabase Setup Menu:")
        print("1. Test connection")
        print("2. Show schema setup instructions")
        print("3. Create sample search URL")
        print("4. Clear all data (DANGER)")
        print("5. Exit")
        
        choice = input("\nSelect option (1-5): ").strip()
        
        if choice == '1':
            test_supabase_connection()
        elif choice == '2':
            show_schema_instructions()
        elif choice == '3':
            create_sample_search_url()
        elif choice == '4':
            clear_database()
        elif choice == '5':
            print("👋 Goodbye!")
            break
        else:
            print("Invalid choice. Please select 1-5.")

if __name__ == "__main__":
    main()