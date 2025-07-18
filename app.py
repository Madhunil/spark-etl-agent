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
from typing import Dict, Any, Optional

# Ensure application directory is in Python path
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

def create_argument_parser() -> argparse.ArgumentParser:
    """Create comprehensive argument parser."""
    parser = argparse.ArgumentParser(
        description="Enhanced Spark ETL Agent - Production Ready",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
🚀 AVAILABLE JOB TYPES:
  control_m_poc_etl    [POC] Development/testing with row limits
  jcap_pa_etl          [Production] Full workflow with backup/validation/alerts

📖 USAGE EXAMPLES:
  # List all available job types
  %(prog)s --list-job-types
  
  # Run Control M POC ETL (development)
  %(prog)s --job-type control_m_poc_etl --job-id poc_001 --load-date 2025-05-20 --limit 10
  
  # Run JCAP PA ETL (production)
  %(prog)s --job-type jcap_pa_etl --job-id jcap_daily_001 --load-date 2025-05-20
  
  # Run continuously every 5 minutes
  %(prog)s --job-type jcap_pa_etl --job-id jcap_hourly --continuous --interval 300
  
  # Use JSON configuration
  %(prog)s --job-config '{"id": "test_001", "type": "control_m_poc_etl", "limit": 5}'
  
  # Force local development mode
  %(prog)s --local --job-type control_m_poc_etl --job-id dev_test --limit 5
        """
    )
    
    # Job specification
    job_group = parser.add_argument_group('Job Configuration')
    job_group.add_argument("--job-type", type=str,
                          choices=["control_m_poc_etl", "jcap_pa_etl"],
                          help="Type of job to execute")
    job_group.add_argument("--job-id", type=str, help="Unique job identifier")
    job_group.add_argument("--job-config", type=str, help="JSON job configuration string")
    job_group.add_argument("--job-config-file", type=str, help="Path to job configuration file")
    job_group.add_argument("--list-job-types", action="store_true",
                          help="List all supported job types and exit")
    
    # Job parameters
    params_group = parser.add_argument_group('Job Parameters')
    params_group.add_argument("--load-date", type=str, help="Load date (YYYY-MM-DD)")
    params_group.add_argument("--limit", type=int, default=10,
                             help="Row limit for control_m_poc_etl (default: 10)")
    
    # Execution control
    exec_group = parser.add_argument_group('Execution Control')
    exec_group.add_argument("--continuous", action="store_true",
                           help="Run continuously instead of once")
    exec_group.add_argument("--interval", type=int, default=60,
                           help="Interval between runs in seconds (default: 60)")
    exec_group.add_argument("--local", action="store_true",
                           help="Force local mode (overrides auto-detection)")
    exec_group.add_argument("--k8s", action="store_true",
                           help="Force Kubernetes mode (overrides auto-detection)")
    
    # Logging
    logging_group = parser.add_argument_group('Logging')
    logging_group.add_argument("--log-level", type=str, default="INFO",
                              choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
                              help="Logging level (default: INFO)")
    
    return parser

def display_job_types(job_service: JobService) -> None:
    """Display available job types with detailed information."""
    print("\n" + "="*80)
    print("🚀 ENHANCED SPARK ETL AGENT - SUPPORTED JOB TYPES")
    print("="*80)
    
    job_types = job_service.list_supported_job_types()
    
    for i, (job_type, description) in enumerate(job_types.items(), 1):
        print(f"\n{i}. 🔧 {job_type}")
        print(f"   {description}")
        
        # Add usage example
        if job_type == "control_m_poc_etl":
            print(f"   💡 Example: python3 app.py --job-type {job_type} --job-id poc_001 --limit 10")
        elif job_type == "jcap_pa_etl":
            print(f"   💡 Example: python3 app.py --job-type {job_type} --job-id jcap_daily")
    
    print(f"\n{'='*80}")
    print("💡 Use --job-type <type> to specify which job to run")
    print("📖 Use --help for complete usage information")
    print("="*80 + "\n")

def create_job_config_from_args(args) -> Optional[Dict[str, Any]]:
    """Create job configuration from command-line arguments."""
    if not args.job_type or not args.job_id:
        return None
    
    config = {
        "id": args.job_id,
        "name": f"{args.job_type.replace('_', ' ').title()} - {args.job_id}",
        "type": args.job_type,
        "load_date": args.load_date
    }
    
    # Add job-specific parameters
    if args.job_type == "control_m_poc_etl":
        config["limit"] = args.limit
    
    return config

def load_job_config_from_file(file_path: str) -> Dict[str, Any]:
    """Load job configuration from JSON file."""
    try:
        with open(file_path, 'r') as f:
            config = json.load(f)
        return config
    except (FileNotFoundError, json.JSONDecodeError) as e:
        raise RuntimeError(f"Failed to load job config from {file_path}: {str(e)}") from e

def execute_single_job(job_service: JobService, job_config: Dict[str, Any]) -> Dict[str, Any]:
    """Execute a single job with preprocessing."""
    # Set current date if load_date is not specified
    if not job_config.get("load_date"):
        job_config["load_date"] = datetime.now().strftime("%Y-%m-%d")
    
    return job_service.execute_job(job_config)

def run_continuous_jobs(job_service: JobService, job_config: Dict[str, Any], 
                       interval: int) -> Dict[str, Any]:
    """Run jobs continuously with comprehensive monitoring."""
    from loguru import logger
    
    logger.info("🔄 Starting continuous job execution")
    
    stats = {
        "total_runs": 0,
        "successful_runs": 0,
        "failed_runs": 0,
        "total_rows_processed": 0,
        "start_time": datetime.now()
    }
    
    while not shutdown_requested:
        stats["total_runs"] += 1
        run_number = stats["total_runs"]
        
        logger.info(f"🎬 Starting job execution #{run_number}")
        
        try:
            result = execute_single_job(job_service, job_config.copy())
            
            if result["status"] == "Success":
                stats["successful_runs"] += 1
                rows_processed = result.get("rows_processed", 0)
                stats["total_rows_processed"] += rows_processed
                
                logger.info(f"✅ Run #{run_number} completed successfully!")
                logger.info(f"📊 Processed {rows_processed:,} rows")
                
                # Log job-specific metrics
                if "variance_percentage" in result:
                    variance = result["variance_percentage"]
                    threshold_exceeded = result.get("variance_threshold_exceeded", False)
                    logger.info(f"📈 Data variance: {variance:.2f}%")
                    
                    if threshold_exceeded:
                        email_sent = result.get("email_sent", False)
                        logger.warning(f"⚠️ Variance alert sent: {email_sent}")
            else:
                stats["failed_runs"] += 1
                error = result.get("error", "Unknown error")
                logger.error(f"❌ Run #{run_number} failed: {error}")
            
            # Log cumulative stats
            success_rate = (stats["successful_runs"] / stats["total_runs"]) * 100
            logger.info(f"📈 Stats: {stats['successful_runs']}/{stats['total_runs']} "
                       f"({success_rate:.1f}% success), "
                       f"{stats['total_rows_processed']:,} total rows")
            
        except Exception as e:
            stats["failed_runs"] += 1
            logger.exception(f"💥 Unhandled exception in run #{run_number}")
        
        # Wait for next execution
        if not shutdown_requested:
            logger.info(f"⏳ Waiting {interval} seconds before next run...")
            for _ in range(interval):
                if shutdown_requested:
                    break
                time.sleep(1)
    
    # Final statistics
    total_duration = (datetime.now() - stats["start_time"]).total_seconds()
    success_rate = (stats["successful_runs"] / stats["total_runs"]) * 100 if stats["total_runs"] > 0 else 0
    
    logger.info("🏁 Continuous execution completed")
    logger.info(f"📊 Final Statistics:")
    logger.info(f"   Total Runs: {stats['total_runs']}")
    logger.info(f"   Successful: {stats['successful_runs']} ({success_rate:.1f}%)")
    logger.info(f"   Failed: {stats['failed_runs']}")
    logger.info(f"   Total Rows: {stats['total_rows_processed']:,}")
    logger.info(f"   Duration: {total_duration:.2f} seconds")
    
    return stats

def main() -> int:
    """Enhanced main application entry point."""
    # Set up signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Parse arguments
    parser = create_argument_parser()
    args = parser.parse_args()
    
    # Set up logging
    setup_logging(log_level=args.log_level)
    
    from loguru import logger
    logger.info("Starting JPH Spark ETL agent v1.2")
    
    if args.continuous:
        logger.info(f"🔄 Continuous mode enabled (interval: {args.interval}s)")
    else:
        logger.info("🎯 Single execution mode")
    
    try:
        # Load settings
        settings = get_settings()
        logger.info("✅ Application settings loaded")
        
        # Determine execution mode
        local_mode = None
        if args.local and args.k8s:
            logger.warning("Both --local and --k8s specified, using auto-detection")
        elif args.local:
            local_mode = True
            logger.info("🏠 Forcing local mode")
        elif args.k8s:
            local_mode = False
            logger.info("☸️ Forcing Kubernetes mode")
        
        # Initialize Spark
        spark_manager = SparkManager(local_mode=local_mode)
        logger.info("⚡ Initializing Spark session...")
        spark = spark_manager.create_spark_session()
        
        # Initialize job service
        job_service = JobService(spark)
        
        # Handle list job types
        if args.list_job_types:
            display_job_types(job_service)
            return 0
        
        # Determine job configuration
        job_config = None
        
        if args.job_config:
            try:
                job_config = json.loads(args.job_config)
                logger.info("📋 Using JSON job configuration from command line")
            except json.JSONDecodeError as e:
                logger.error(f"❌ Invalid JSON configuration: {str(e)}")
                return 1
        
        elif args.job_config_file:
            try:
                job_config = load_job_config_from_file(args.job_config_file)
                logger.info(f"📄 Loaded job configuration from {args.job_config_file}")
            except RuntimeError as e:
                logger.error(f"❌ {str(e)}")
                return 1
        
        else:
            job_config = create_job_config_from_args(args)
            if job_config:
                logger.info(f"📝 Created configuration: {args.job_type} - {args.job_id}")
            else:
                logger.error("❌ No job configuration provided")
                logger.info("💡 Examples:")
                logger.info("   python3 app.py --job-type control_m_poc_etl --job-id poc_001 --limit 10")
                logger.info("   python3 app.py --job-type jcap_pa_etl --job-id jcap_daily")
                logger.info("   python3 app.py --list-job-types")
                return 1
        
        # Validate configuration
        required_fields = ["type", "id"]
        missing_fields = [field for field in required_fields if field not in job_config]
        
        if missing_fields:
            logger.error(f"❌ Missing required fields: {missing_fields}")
            return 1
        
        # Log safe configuration
        safe_config = {k: v for k, v in job_config.items() 
                      if not any(sensitive in k.lower() for sensitive in ["password", "secret", "key"])}
        logger.info(f"🎯 Job configuration: {safe_config}")
        
        # Execute job(s)
        if args.continuous:
            stats = run_continuous_jobs(job_service, job_config, args.interval)
            success_rate = (stats["successful_runs"] / stats["total_runs"]) * 100 if stats["total_runs"] > 0 else 0
            return 0 if success_rate >= 50 else 1  # Consider 50%+ success rate as overall success
        else:
            result = execute_single_job(job_service, job_config)
            
            if result["status"] == "Success":
                rows = result.get("rows_processed", 0)
                duration = result.get("duration_seconds", 0)
                
                logger.info(f"🎉 Job completed successfully!")
                logger.info(f"📊 Processed {rows:,} rows in {duration:.2f} seconds")
                
                # Log additional metrics
                if "variance_percentage" in result:
                    variance = result["variance_percentage"]
                    threshold_exceeded = result.get("variance_threshold_exceeded", False)
                    logger.info(f"📈 Data variance: {variance:.2f}%")
                    
                    if threshold_exceeded:
                        email_sent = result.get("email_sent", False)
                        logger.warning(f"⚠️ Variance threshold exceeded - Alert sent: {email_sent}")
                
                return 0
            else:
                error = result.get("error", "Unknown error")
                logger.error(f"💥 Job failed: {error}")
                return 1
    
    except KeyboardInterrupt:
        logger.warning("⏸️ Execution interrupted by user")
        return 130
    
    except Exception as e:
        logger.exception(f"💥 Unhandled application error: {str(e)}")
        return 1
    
    finally:
        # Cleanup
        if 'spark_manager' in locals() and hasattr(spark_manager, 'spark') and spark_manager.spark:
            logger.info("🧹 Cleaning up Spark resources...")
            spark_manager.stop_spark_session()

if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)