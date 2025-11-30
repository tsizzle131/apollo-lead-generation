"""
API Logger Module
Comprehensive logging for all API calls to track requests, responses, and costs
"""

import logging
import json
import time
from datetime import datetime
from typing import Dict, Any, Optional
import os

class APILogger:
    """Centralized API call logger for audit and optimization"""

    def __init__(self, log_file: Optional[str] = None):
        """
        Initialize API logger

        Args:
            log_file: Optional file path for detailed logs (JSON format)
        """
        self.log_file = log_file
        self.call_history = []

        # Create log directory if needed
        if log_file:
            log_dir = os.path.dirname(log_file)
            if log_dir and not os.path.exists(log_dir):
                os.makedirs(log_dir)

    def log_api_call(self,
                     service: str,
                     operation: str,
                     request_data: Dict[str, Any],
                     response_data: Optional[Dict[str, Any]] = None,
                     duration_ms: Optional[float] = None,
                     cost_usd: Optional[float] = None,
                     error: Optional[str] = None):
        """
        Log an API call with full details

        Args:
            service: Service name (e.g., "apify", "openai", "bouncer")
            operation: Operation type (e.g., "google_maps_scrape", "facebook_enrich")
            request_data: Request payload/parameters
            response_data: Response data (summary)
            duration_ms: Duration in milliseconds
            cost_usd: Estimated cost in USD
            error: Error message if failed
        """
        timestamp = datetime.now().isoformat()

        # Create log entry
        log_entry = {
            "timestamp": timestamp,
            "service": service,
            "operation": operation,
            "request": request_data,
            "response_summary": self._summarize_response(response_data) if response_data else None,
            "duration_ms": duration_ms,
            "cost_usd": cost_usd,
            "error": error,
            "status": "error" if error else "success"
        }

        # Add to history
        self.call_history.append(log_entry)

        # Log to console
        self._log_to_console(log_entry)

        # Log to file if configured
        if self.log_file:
            self._log_to_file(log_entry)

    def _summarize_response(self, response_data: Dict[str, Any]) -> Dict[str, Any]:
        """Create a summary of response data"""
        summary = {}

        # For list responses, show count and sample
        if isinstance(response_data, list):
            summary["count"] = len(response_data)
            summary["sample"] = response_data[0] if response_data else None
        elif isinstance(response_data, dict):
            # Show key fields
            summary["keys"] = list(response_data.keys())

            # Count items if present
            for key in ["items", "results", "data", "businesses"]:
                if key in response_data and isinstance(response_data[key], list):
                    summary[f"{key}_count"] = len(response_data[key])

        return summary

    def _log_to_console(self, log_entry: Dict[str, Any]):
        """Log to console with formatted output"""
        service = log_entry["service"]
        operation = log_entry["operation"]
        status = log_entry["status"]

        # Header
        logging.info("=" * 80)
        logging.info(f"🔌 API CALL: {service.upper()} - {operation}")
        logging.info("=" * 80)

        # Request details
        logging.info("📤 REQUEST:")
        request_str = json.dumps(log_entry["request"], indent=2)
        for line in request_str.split('\n'):
            logging.info(f"   {line}")

        # Response details
        if log_entry.get("response_summary"):
            logging.info("\n📥 RESPONSE SUMMARY:")
            response_str = json.dumps(log_entry["response_summary"], indent=2)
            for line in response_str.split('\n'):
                logging.info(f"   {line}")

        # Metrics
        logging.info("\n📊 METRICS:")
        if log_entry.get("duration_ms"):
            logging.info(f"   ⏱️  Duration: {log_entry['duration_ms']:.2f}ms ({log_entry['duration_ms']/1000:.2f}s)")
        if log_entry.get("cost_usd"):
            logging.info(f"   💰 Cost: ${log_entry['cost_usd']:.4f}")

        # Status
        status_icon = "✅" if status == "success" else "❌"
        logging.info(f"\n{status_icon} STATUS: {status.upper()}")

        if log_entry.get("error"):
            logging.error(f"   Error: {log_entry['error']}")

        logging.info("=" * 80 + "\n")

    def _log_to_file(self, log_entry: Dict[str, Any]):
        """Append log entry to file"""
        try:
            with open(self.log_file, 'a') as f:
                f.write(json.dumps(log_entry) + '\n')
        except Exception as e:
            logging.warning(f"Failed to write to log file: {e}")

    def log_batch_operation(self,
                           service: str,
                           operation: str,
                           batch_size: int,
                           total_items: int,
                           batch_num: int,
                           total_batches: int):
        """Log batch operation details"""
        logging.info("=" * 80)
        logging.info(f"📦 BATCH OPERATION: {service.upper()} - {operation}")
        logging.info("=" * 80)
        logging.info(f"   Batch: {batch_num}/{total_batches}")
        logging.info(f"   Batch Size: {batch_size} items")
        logging.info(f"   Total Items: {total_items}")
        logging.info(f"   Progress: {(batch_num/total_batches)*100:.1f}%")
        logging.info("=" * 80 + "\n")

    def get_summary_stats(self) -> Dict[str, Any]:
        """Get summary statistics of all API calls"""
        if not self.call_history:
            return {"total_calls": 0}

        stats = {
            "total_calls": len(self.call_history),
            "by_service": {},
            "by_operation": {},
            "total_cost_usd": 0,
            "total_duration_ms": 0,
            "error_count": 0,
            "success_count": 0
        }

        for entry in self.call_history:
            # Count by service
            service = entry["service"]
            stats["by_service"][service] = stats["by_service"].get(service, 0) + 1

            # Count by operation
            operation = entry["operation"]
            stats["by_operation"][operation] = stats["by_operation"].get(operation, 0) + 1

            # Sum costs and duration
            if entry.get("cost_usd"):
                stats["total_cost_usd"] += entry["cost_usd"]
            if entry.get("duration_ms"):
                stats["total_duration_ms"] += entry["duration_ms"]

            # Count status
            if entry["status"] == "error":
                stats["error_count"] += 1
            else:
                stats["success_count"] += 1

        return stats

    def print_summary(self):
        """Print summary statistics to console"""
        stats = self.get_summary_stats()

        logging.info("\n" + "=" * 80)
        logging.info("📊 API CALL SUMMARY")
        logging.info("=" * 80)
        logging.info(f"Total Calls: {stats['total_calls']}")
        logging.info(f"Successful: {stats['success_count']}")
        logging.info(f"Failed: {stats['error_count']}")
        logging.info(f"Total Cost: ${stats['total_cost_usd']:.2f}")
        logging.info(f"Total Duration: {stats['total_duration_ms']/1000:.2f}s")

        logging.info("\n📦 By Service:")
        for service, count in stats["by_service"].items():
            logging.info(f"   {service}: {count} calls")

        logging.info("\n🔧 By Operation:")
        for operation, count in stats["by_operation"].items():
            logging.info(f"   {operation}: {count} calls")

        logging.info("=" * 80 + "\n")


# Global logger instance
_global_logger = None

def get_api_logger(log_file: Optional[str] = None) -> APILogger:
    """Get or create global API logger instance"""
    global _global_logger
    if _global_logger is None:
        _global_logger = APILogger(log_file)
    return _global_logger
