#!/usr/bin/env python3
"""
Main entry point for the Spark ETL Agent.
Implements a modular Python agent application for running Spark ETL jobs on Kubernetes.
"""
import sys
import os
import argparse
import json
import time
import signal
from datetime import datetime

# Ensure the application directory is in the Python path
current_dir = os.path.dirname(os.path.abspath(__file__))
if current_dir not in sys.path:
    sys.path.insert(0, current_dir)

# Import application modules
from core.logging import setup_logging
from core.config import get_settings
from core.spark import SparkManager
from services.job_service import JobService

# Global flag for graceful shutdown
shutdown_requested = False

def signal_handler(signum, frame):
    """Handle shutdown signals gracefully."""
    global shutdown_requested
    shutdown_requested = True
    print("\n🛑 Shutdown signal received. Finishing current job and exiting...")

def parse_arguments():
    """
    Parse command-line arguments for the application.
    
    Returns:
        argparse.Namespace: Parsed command-line arguments
    """
    parser = argparse.ArgumentParser(
        description="Spark ETL Application - Control M POC ETL Job",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Run Control M POC ETL job once
  %(prog)s --job-id opsumit_001 --load-date 2025-05-20 --limit 10
  
  # Run continuously every minute
  %(prog)s --job-id opsumit_001 --limit 10 --continuous
  
  # Run continuously every 30 seconds
  %(prog)s --job-id opsumit_001 --limit 5 --continuous --interval 30
  
  # Run with JSON configuration continuously
  %(prog)s --job-config '{"id": "test_001", "limit": 5}' --continuous
  
  # Force local mode for development
  %(prog)s --local --job-id dev_001 --load-date 2025-05-20
        """
    )
    
    # Job specification
    parser.add_argument("--job-id", type=str, help="Job ID to execute")
    parser.add_argument("--job-config", type=str, help="Job configuration JSON string")
    parser.add_argument("--job-config-file", type=str, help="Path to job configuration file")
    
    # Job parameters
    parser.add_argument("--load-date", type=str, help="Load date (YYYY-MM-DD)")
    parser.add_argument("--limit", type=int, default=10, help="Limit the number of rows to process (default: 10)")
    
    # Continuous execution
    parser.add_argument("--continuous", action="store_true", help="Run continuously instead of once")
    parser.add_argument("--interval", type=int, default=60, help="Interval between runs in seconds (default: 60)")
    
    # Execution control
    parser.add_argument("--local", action="store_true", help="Force local mode (overrides auto-detection)")
    parser.add_argument("--k8s", action="store_true", help="Force Kubernetes mode (overrides auto-detection)")
    
    # Logging
    parser.add_argument("--log-level", type=str, default="INFO", 
                        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                        help="Logging level (default: INFO)")
    
    return parser.parse_args()

def run_single_job(job_service, job_config):
    """
    Run a single job execution.
    
    Args:
        job_service: JobService instance
        job_config: Job configuration dictionary
    
    Returns:
        dict: Job execution results
    """
    # Update load_date to current date if not specified in continuous mode
    if "load_date" not in job_config or not job_config["load_date"]:
        job_config["load_date"] = datetime.now().strftime("%Y-%m-%d")
    
    # Execute the job
    result = job_service.execute_job(job_config)
    return result

def main():
    """
    Main application entry point.
    
    Returns:
        int: Exit code (0 for success, non-zero for failure)
    """
    # Set up signal handlers for graceful shutdown
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse command-line arguments
    args = parse_arguments()
    
    # Set up logging
    setup_logging(log_level=args.log_level)
    
    # Import logger after setup
    from loguru import logger
    logger.info("🚀 Starting Spark ETL agent at 30-06 341")
    
    if args.continuous:
        logger.info(f"🔄 Running in continuous mode with {args.interval}s interval")
    else:
        logger.info("🎯 Running in single execution mode")
    
    # Get application settings
    try:
        settings = get_settings()
        logger.info("✅ Application settings loaded successfully")
    except Exception as e:
        logger.error(f"❌ Failed to load application settings: {str(e)}")
        return 1
    
    # Determine execution mode
    local_mode = None  # None means auto-detect
    if args.local and args.k8s:
        logger.warning("Both --local and --k8s specified, using auto-detection")
    elif args.local:
        local_mode = True
        logger.info("🏠 Forcing local mode due to --local flag")
    elif args.k8s:
        local_mode = False
        logger.info("☸️ Forcing Kubernetes mode due to --k8s flag")
    
    # Create Spark session manager
    spark_manager = SparkManager(local_mode=local_mode)
    
    try:
        # Create Spark session
        logger.info("⚡ Initializing Spark session...")
        spark = spark_manager.create_spark_session()
        
        # Initialize job service
        job_service = JobService(spark)
        
        # Determine job configuration
        job_config = None
        
        if args.job_config:
            # Parse job configuration from command-line JSON string
            try:
                job_config = json.loads(args.job_config)
                logger.info("📋 Using job configuration from command-line argument")
            except json.JSONDecodeError as e:
                logger.error(f"❌ Error parsing job configuration JSON: {str(e)}")
                return 1
        
        elif args.job_config_file:
            # Load job configuration from file
            try:
                with open(args.job_config_file, 'r') as f:
                    job_config = json.load(f)
                logger.info(f"📄 Loaded job configuration from file: {args.job_config_file}")
            except (json.JSONDecodeError, FileNotFoundError) as e:
                logger.error(f"❌ Error loading job configuration file: {str(e)}")
                return 1
        
        elif args.job_id:
            # Create a simple configuration based on job ID
            job_config = {
                "id": args.job_id,
                "name": f"Control M POC ETL Job {args.job_id}",
                "type": "control_m_poc_etl",
                "load_date": args.load_date,  # Will be set to current date if None in continuous mode
                "limit": args.limit
            }
            logger.info(f"📝 Created job configuration for job ID: {args.job_id}")
        
        else:
            logger.error("❌ No job configuration provided. Use --job-id, --job-config, or --job-config-file")
            logger.info("💡 Example: python3 app.py --job-id opsumit_001 --load-date 2025-05-20 --limit 10")
            return 1
        
        # Ensure job type is set
        if "type" not in job_config:
            job_config["type"] = "control_m_poc_etl"
        
        # Log job configuration (safe version)
        safe_config = {k: v for k, v in job_config.items() if k != "password"}
        logger.info(f"🎯 Job configuration: {safe_config}")
        
        # Execute job(s)
        if args.continuous:
            # Continuous execution mode
            logger.info("🔄 Starting continuous job execution...")
            run_count = 0
            total_rows_processed = 0
            
            while not shutdown_requested:
                run_count += 1
                logger.info(f"🎬 Starting job execution #{run_count}")
                
                try:
                    result = run_single_job(job_service, job_config.copy())
                    
                    if result["status"] == "Success":
                        rows_processed = result.get('rows_processed', 0)
                        total_rows_processed += rows_processed
                        logger.info(f"✅ Run #{run_count} completed successfully! Processed {rows_processed} rows.")
                        logger.info(f"📊 Total rows processed so far: {total_rows_processed}")
                    else:
                        logger.error(f"❌ Run #{run_count} failed: {result.get('error', 'Unknown error')}")
                        # Continue running even if one job fails
                    
                except Exception as e:
                    logger.exception(f"💥 Unhandled exception in run #{run_count}")
                    # Continue running even if one job fails
                
                # Wait for the specified interval before next run
                if not shutdown_requested:
                    logger.info(f"⏳ Waiting {args.interval} seconds before next run...")
                    for _ in range(args.interval):
                        if shutdown_requested:
                            break
                        time.sleep(1)
            
            logger.info(f"🏁 Continuous execution stopped. Completed {run_count} runs, processed {total_rows_processed} total rows.")
            return 0
        
        else:
            # Single execution mode
            logger.info("🎬 Starting single job execution...")
            result = run_single_job(job_service, job_config)
            
            # Return appropriate exit code based on job status
            if result["status"] == "Success":
                logger.info(f"🎉 Job completed successfully! Processed {result.get('rows_processed', 0)} rows.")
                return 0
            else:
                logger.error(f"💥 Job failed: {result.get('error', 'Unknown error')}")
                return 1
        
    except KeyboardInterrupt:
        logger.warning("⏸️ Job execution interrupted by user")
        return 130  # Standard exit code for SIGINT
        
    except Exception as e:
        logger.exception(f"💥 Unhandled exception in main application")
        return 1
    
    finally:
        # Clean up resources
        if 'spark_manager' in locals() and hasattr(spark_manager, 'spark') and spark_manager.spark:
            logger.info("🧹 Cleaning up resources...")
            spark_manager.stop_spark_session()
    
    return 0

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)