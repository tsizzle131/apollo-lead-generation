#!/usr/bin/env python3
"""
Scheduler for Lead Generation System

This script runs the lead generation workflow on a schedule,
replicating the n8n Schedule Trigger functionality.
"""

import schedule
import time
import logging
import signal
import sys
from datetime import datetime
from main import LeadGenerationOrchestrator
import config

class LeadGenerationScheduler:
    def __init__(self):
        self.orchestrator = LeadGenerationOrchestrator()
        self.running = True
        
        # Set up signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """Handle shutdown signals gracefully"""
        logging.info(f"Received signal {signum}, shutting down gracefully...")
        self.running = False
    
    def run_scheduled_workflow(self):
        """Run the workflow with error handling and logging"""
        try:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            logging.info(f"⏰ Scheduled workflow starting at {timestamp}")
            
            success = self.orchestrator.run_workflow()
            
            if success:
                logging.info("✅ Scheduled workflow completed successfully")
            else:
                logging.error("❌ Scheduled workflow failed")
                
        except Exception as e:
            logging.error(f"❌ Scheduled workflow error: {e}")
    
    def start_scheduler(self):
        """Start the scheduler with the configured interval"""
        interval_minutes = config.SCHEDULE_INTERVAL_MINUTES
        
        logging.info(f"🕐 Starting scheduler - will run every {interval_minutes} minutes")
        logging.info("Press Ctrl+C to stop the scheduler")
        
        # Schedule the workflow
        schedule.every(interval_minutes).minutes.do(self.run_scheduled_workflow)
        
        # Run once immediately
        logging.info("🚀 Running initial workflow...")
        self.run_scheduled_workflow()
        
        # Keep running scheduled jobs
        while self.running:
            try:
                schedule.run_pending()
                time.sleep(1)
            except KeyboardInterrupt:
                break
            except Exception as e:
                logging.error(f"Scheduler error: {e}")
                time.sleep(60)  # Wait a minute before retrying
        
        logging.info("📴 Scheduler stopped")

def main():
    """Main entry point for scheduler"""
    # Configure logging
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('scheduler.log'),
            logging.StreamHandler()
        ]
    )
    
    scheduler = LeadGenerationScheduler()
    
    # Test connections before starting
    logging.info("🧪 Testing connections before starting scheduler...")
    if not scheduler.orchestrator.test_connections():
        logging.error("❌ Connection tests failed. Please check your API keys and configuration.")
        sys.exit(1)
    
    # Start the scheduler
    scheduler.start_scheduler()

if __name__ == "__main__":
    main()