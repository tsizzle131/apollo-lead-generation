#!/usr/bin/env python3
"""Test script to verify the new human-like icebreaker prompt"""

import json
import sys
import os
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

# Import modules
from config import reload_config, ICEBREAKER_PROMPT, AI_MODEL_ICEBREAKER, AI_TEMPERATURE
from modules.ai_generator import generate_icebreaker

def test_new_prompt():
    """Test the new icebreaker prompt with sample data"""
    
    # Reload config to get latest prompt
    reload_config()
    
    print("=" * 70)
    print("🧪 TESTING NEW HUMAN-LIKE ICEBREAKER PROMPT")
    print("=" * 70)
    print()
    
    # Test cases with different types of businesses
    test_cases = [
        {
            "contact": {
                "first_name": "Sarah",
                "last_name": "Johnson",
                "headline": "VP of Marketing",
                "company_name": "TechFlow Solutions",
                "location": "San Francisco, CA"
            },
            "summaries": [
                "TechFlow Solutions provides cloud-based workflow automation tools for mid-market companies. They recently announced a Series B funding round of $25M to expand their enterprise features and API integrations.",
                "The company focuses on helping businesses streamline their operations through AI-powered process optimization. Their platform integrates with over 50 popular business tools including Salesforce, Slack, and Microsoft Teams."
            ]
        },
        {
            "contact": {
                "first_name": "Mike",
                "last_name": "Chen",
                "headline": "CEO",
                "company_name": "DataSync Inc",
                "location": "Austin, TX"
            },
            "summaries": [
                "DataSync Inc specializes in real-time data synchronization for e-commerce businesses. They help online retailers keep inventory consistent across multiple sales channels.",
                "The company was founded in 2021 and has grown to serve over 500 clients. They recently launched a new feature for automated pricing adjustments based on competitor analysis."
            ]
        },
        {
            "contact": {
                "first_name": "Jennifer",
                "last_name": "Williams",
                "headline": "Director of Sales",
                "company_name": "GreenTech Solutions",
                "location": "Denver, CO"
            },
            "summaries": [
                "GreenTech Solutions develops sustainable energy management software for commercial buildings. Their platform helps property managers reduce energy costs by up to 30%.",
                "They work primarily with office buildings and retail spaces in major metropolitan areas. The company emphasizes both environmental impact and ROI for their clients."
            ]
        }
    ]
    
    print(f"📋 Model: {AI_MODEL_ICEBREAKER}")
    print(f"🌡️  Temperature: {AI_TEMPERATURE}")
    print()
    print("Current Prompt Preview (first 500 chars):")
    print("-" * 50)
    print(ICEBREAKER_PROMPT[:500] + "...")
    print("-" * 50)
    print()
    
    # Test each case
    for i, test_case in enumerate(test_cases, 1):
        contact = test_case["contact"]
        summaries = test_case["summaries"]
        
        print(f"\n{'='*70}")
        print(f"TEST CASE {i}: {contact['first_name']} {contact['last_name']} at {contact['company_name']}")
        print(f"Role: {contact['headline']}")
        print(f"Location: {contact['location']}")
        print("=" * 70)
        
        print("\n📄 Website Summaries:")
        for j, summary in enumerate(summaries, 1):
            print(f"\n{j}. {summary}")
        
        print("\n🤖 Generating icebreaker with new prompt...")
        
        try:
            # Generate icebreaker
            result = generate_icebreaker(contact, summaries)
            
            if result and 'icebreaker' in result:
                print("\n✅ Generated Icebreaker:")
                print("-" * 50)
                print(result['icebreaker'])
                print("-" * 50)
                
                # Analyze the quality
                icebreaker = result['icebreaker'].lower()
                print("\n📊 Quality Check:")
                
                # Check for human-like language
                human_phrases = ["saw you", "noticed", "came across", "caught my eye", "stumbled on", "checking out"]
                has_human_phrase = any(phrase in icebreaker for phrase in human_phrases)
                print(f"  ✓ Human-like language: {'Yes' if has_human_phrase else 'No'}")
                
                # Check for casual tone
                casual_indicators = ["guys", "folks", "cool", "interesting", "curious", "worth exploring", "might be useful"]
                has_casual_tone = any(indicator in icebreaker for indicator in casual_indicators)
                print(f"  ✓ Casual tone: {'Yes' if has_casual_tone else 'No'}")
                
                # Check for specificity (not too generic)
                generic_phrases = ["love your website", "great company", "impressive work", "amazing"]
                has_generic = any(phrase in icebreaker for phrase in generic_phrases)
                print(f"  ✓ Avoids generic phrases: {'Yes' if not has_generic else 'No'}")
                
                # Check length (should be concise)
                word_count = len(icebreaker.split())
                is_concise = 20 <= word_count <= 60
                print(f"  ✓ Concise (20-60 words): {'Yes' if is_concise else 'No'} ({word_count} words)")
                
            else:
                print("\n❌ Error: No icebreaker generated")
                print(f"Result: {result}")
        
        except Exception as e:
            print(f"\n❌ Error generating icebreaker: {e}")
        
        print()
    
    print("\n" + "=" * 70)
    print("✅ TEST COMPLETE")
    print("=" * 70)

if __name__ == "__main__":
    test_new_prompt()