"""
Enhanced JCAP PA ETL service with full production features.
"""
from typing import Dict, Any, Optional
from datetime import datetime
from loguru import logger
from pyspark.sql.functions import col, to_timestamp, lit
from utils.db_utils import RedshiftConnector
from services.s3_service import S3Service
from services.email_service import EmailService
from core.config import get_settings

class JcapPaEtlService:
    """
    Production JCAP PA ETL service with comprehensive workflow management.
    """
    
    def __init__(self, spark):
        """Initialize JCAP PA ETL service."""
        self.spark = spark
        self.settings = get_settings()
        
        # Initialize connectors for different environments
        self.cdp_connector = RedshiftConnector(spark, connection_type="cdp")
        self.jcap_connector = RedshiftConnector(spark, connection_type="jcap")
        self.s3_service = S3Service(spark)
        self.email_service = EmailService()
        
        # Configuration
        self.main_table = "jcap_pa"
        self.backup_table = "jcap_pa_bkp"
        self.schema = self.settings.JCAP_REDSHIFT_SCHEMA
        self.s3_path = f"s3://{self.settings.S3_BUCKET}/jcap_pa_dashboard/"
        
        logger.info("🏭 JCAP PA ETL Service initialized (Production)")
        logger.info(f"📋 CDP Source: {self.cdp_connector.connection_type}")
        logger.info(f"📋 JCAP Destination: {self.jcap_connector.connection_type}")
        logger.info(f"💾 S3 Staging: {self.s3_path}")
    
    def run_jcap_pa_etl(self, load_date: Optional[str] = None) -> Dict[str, Any]:
        """Execute complete JCAP PA ETL workflow."""
        try:
            logger.info("🚀 Starting JCAP PA ETL (Production Workflow)")
            start_time = datetime.now()
            
            if not load_date:
                load_date = datetime.now().strftime("%Y-%m-%d")
            
            logger.info(f"📅 Load date: {load_date}")
            
            # Step 1: Create and validate backup
            logger.info("1️⃣ Creating backup and validation")
            previous_count = self._create_and_validate_backup()
            
            # Step 2: Execute CDP data extraction
            logger.info("2️⃣ Extracting data from CDP")
            df = self._extract_cdp_data()
            
            # Step 3: Transform data
            logger.info("3️⃣ Transforming data")
            df_transformed = self._transform_data(df)
            
            # Step 4: Stage to S3
            logger.info("4️⃣ Staging data to S3")
            self._stage_to_s3(df_transformed)

            # Step 5: Load to destination (FIXED)
            logger.info("5️⃣ Loading to destination")
            self._load_to_destination(df_transformed)
            
            # Step 6: Validate and alert
            logger.info("6️⃣ Validating results")
            current_count = self.jcap_connector.get_table_count(self.main_table, self.schema)
            variance_result = self._validate_and_alert(previous_count, current_count)
            
            # Calculate final metrics
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            logger.info("✅ JCAP PA ETL completed successfully!")
            logger.info(f"📊 Processed {current_count:,} rows in {duration:.2f} seconds")
            logger.info(f"📈 Variance: {variance_result['variance_percentage']:.2f}%")

            # Send success notification
            self.email_service.send_job_completion_notification(
                job_name="JCAP PA ETL",
                status="Success",
                duration=duration
            )

            return {
                "status": "Success",
                "rows_processed": current_count,
                "start_time": start_time,
                "end_time": end_time,
                "duration_seconds": duration,
                "load_date": load_date,
                "previous_count": previous_count,
                "current_count": current_count,
                "variance_percentage": variance_result['variance_percentage'],
                "variance_threshold_exceeded": variance_result['threshold_exceeded'],
                "email_sent": variance_result['email_sent'],
                "s3_path": self.s3_path,
                "method": "Production Spark Workflow"
            }
            
        except Exception as e:
            logger.exception(f"❌ JCAP PA ETL failed: {str(e)}")
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds() if 'start_time' in locals() else 0
            
            # Send failure notification
            self.email_service.send_job_completion_notification(
                job_name="JCAP PA ETL",
                status="Failed",
                duration=duration,
                error_message=str(e)
            )
            
            return {
                "status": "Failed",
                "error": str(e),
                "start_time": locals().get('start_time', datetime.now()),
                "end_time": end_time,
                "duration_seconds": duration
            }
    
    def _create_and_validate_backup(self) -> int:
        """FIXED: Create backup with comprehensive validation using corrected methods."""
        try:
            # Truncate backup table using fixed method
            logger.info("🗑️ Truncating backup table")
            self.jcap_connector.truncate_table(self.backup_table, self.schema)
            #logger.info("🗑️ Skip Truncating backup table")
            
            # Get original count
            original_count = self.jcap_connector.get_table_count(self.main_table, self.schema)
            logger.info(f"📊 Original count: {original_count:,}")
            
            if original_count == 0:
                logger.warning("⚠️ Main table is empty - skipping backup")
                return 0
            
            # Copy data to backup using fixed method
            logger.info("🔄 Copying data to backup")
            copied_rows = self.jcap_connector.copy_table_data(
                source_table=self.main_table,
                dest_table=self.backup_table,
                source_schema=self.schema,
                dest_schema=self.schema
            )
            
            # Validate backup
            backup_count = self.jcap_connector.get_table_count(self.backup_table, self.schema)
            
            if original_count != backup_count:
                raise RuntimeError(
                    f"Backup validation failed: Original={original_count:,}, "
                    f"Backup={backup_count:,}, Copied={copied_rows:,}"
                )
            
            logger.info(f"✅ Backup created and validated: {backup_count:,} rows")
            return original_count
            
        except Exception as e:
            logger.exception(f"❌ Backup creation failed: {str(e)}")
            raise RuntimeError(f"Backup creation failed: {str(e)}") from e
    
    def _extract_cdp_data(self):
        """Extract data from CDP with optimized query."""
        try:
            # Enhanced CDP query with PROPER Redshift syntax
            cdp_query = """
            SELECT DISTINCT
                CURRENT_DATE::date AS JCAP_table_loaddate,
                p.pmc_patid::varchar  as pmc_patid,
                U.MANAGING_HCP_STATE AS REFERRING_HCP_PATH_STATE,
                P.prod_nm AS DrugorTherapy,
                pa_completed_date::Date AS PA_CompletedDate,
                pa_initiated_date::Date AS PA_InitiatedDate,
                p.pa_disposition AS PADisposition,
                P.Appeal_Disposition AS AppealDisposition,
                P.FE_REquired AS FEREquired,
                P.rx_PlanName AS rx_PlanName,
                P.rx_PayerName AS rx_PayerName,
                P.rx_PayerType AS rx_PayerType,
                p.sr_type AS srtype,
                p.load_date::Date AS load_date,
                p.ins_planname AS insurancebenefitplanname,
                p.pbm_name AS pbmpayername,
                C.LHM_Name,
                c.bd_terrname AS region,
                S.dynamic_segment AS segment
            FROM (
                SELECT * FROM cdp.fct_pah_pa_payer_details
                WHERE UPPER(prod_nm) IN ('OPSUMIT', 'UPTRAVI', 'OPSYNVI')
                AND pa_disposition IN ('Approved', 'Denied')
            ) P
            LEFT JOIN (
                SELECT DISTINCT 
                    pmc_patid, prod_nm, MANAGING_HCP_STATE,
                    managing_hcp_zip, managing_hcp_jnj_id 
                FROM cdp.fct_pah_ref_cap_dly
            ) U ON P.pmc_patid = U.pmc_PATID 
                AND UPPER(P.prod_nm) = UPPER(U.prod_nm)
            LEFT JOIN (
                SELECT * FROM cdp.dmn_pah_curr_alignment_all
            ) C ON U.managing_hcp_zip = C.zip
            LEFT JOIN (
                SELECT JNJ_ID, Dynamic_Segment 
                FROM cdp.DMN_PAH_SEGMENT 
                WHERE actv_Flag = '1'
            ) S ON U.managing_hcp_jnj_id = S.jnj_id
            WHERE pa_completed_date > '2024-12-31'
            AND pa_completed_date <= CURRENT_DATE
            """
            
            logger.info("🔍 Executing CDP extraction query")
            df = self.cdp_connector.execute_sql(cdp_query)
            
            row_count = df.count()
            logger.info(f"📊 Extracted {row_count:,} rows from CDP")
            
            # Show sample data
            logger.info("📋 Sample extracted data:")
            df.show(5, truncate=False)
            
            return df
            
        except Exception as e:
            logger.exception(f"❌ CDP data extraction failed: {str(e)}")
            raise RuntimeError(f"CDP extraction failed: {str(e)}") from e
    
    def _transform_data(self, df):
        """Transform data with enhanced type handling."""
        try:
            logger.info("🔄 Transforming data types and formats")
            
            # Transform date columns with proper error handling
            # df_transformed = (df
            #     .withColumn("load_date", to_timestamp(col("load_date"), "MM-dd-yyyy"))
            #     .withColumn("PA_CompletedDate", to_timestamp(col("PA_CompletedDate"), "MM-dd-yyyy"))
            #     .withColumn("PA_InitiatedDate", to_timestamp(col("PA_InitiatedDate"), "MM-dd-yyyy"))
            #     .withColumn("JCAP_table_loaddate", to_timestamp(col("JCAP_table_loaddate"), "MM-dd-yyyy"))
            #     .drop("PA_CompletedDate", "PA_InitiatedDate", "JCAP_table_loaddate")  # Remove originals
            # )
            
            df =df.withColumn("load_date", to_timestamp(col("load_date"), "MM-dd-yyyy"))
            df =df.withColumn("PA_CompletedDate", to_timestamp(col("PA_CompletedDate"), "MM-dd-yyyy"))
            df =df.withColumn("PA_InitiatedDate", to_timestamp(col("PA_InitiatedDate"), "MM-dd-yyyy"))
            df =df.withColumn("JCAP_table_loaddate", to_timestamp(col("JCAP_table_loaddate"), "MM-dd-yyyy"))
            df.dtypes

            df_transformed = df


            # Rename remaining columns to match target schema
            column_mapping = {
                "DrugorTherapy": "drugortherapy",
                "PADisposition": "padisposition", 
                "AppealDisposition": "appealdisposition",
                "FEREquired": "ferequired",
                "rx_PlanName": "rx_planname",
                "rx_PayerName": "rx_payername", 
                "rx_PayerType": "rx_payertype",
                "LHM_Name": "lhm_name",
                "REFERRING_HCP_PATH_STATE": "referring_hcp_path_state"
            }
            
            # Apply column name transformations
            for old_col, new_col in column_mapping.items():
                if old_col in df_transformed.columns:
                    df_transformed = df_transformed.withColumnRenamed(old_col, new_col)
            
            # Log schema information
            logger.info("📋 Transformed schema:")
            for field in df_transformed.schema.fields:
                logger.info(f"  {field.name}: {field.dataType}")
            
            return df_transformed
            
        except Exception as e:
            logger.exception(f"❌ Data transformation failed: {str(e)}")
            raise RuntimeError(f"Data transformation failed: {str(e)}") from e
    
    def _stage_to_s3(self, df):
        """Stage data to S3 with optimizations."""
        try:
            logger.info(f"💾 Staging data to S3: {self.s3_path}")
            
            # Clean up existing data
            if self.s3_service.path_exists(self.s3_path):
                logger.info("🗑️ Removing existing S3 data")
                self.s3_service.delete_path(self.s3_path)
            
            # Write optimized Parquet
            self.s3_service.write_parquet(
                df=df,
                s3_path=self.s3_path,
                mode='overwrite'
            )
            
            logger.info("✅ Data staged to S3 successfully")
            
        except Exception as e:
            logger.exception(f"❌ S3 staging failed: {str(e)}")
            raise RuntimeError(f"S3 staging failed: {str(e)}") from e
    
    def _load_to_destination(self, df_transformed):
        """FIXED: Load data using direct Spark JDBC write - exactly like notebook."""
        try:
            # Truncate main table using fixed method
            logger.info("🗑️ Truncating destination table")
            self.jcap_connector.truncate_table(self.main_table, self.schema)
            
            # Get row count before write
            row_count = df_transformed.count()
            logger.info(f"📥 Loading {row_count:,} rows using direct Spark JDBC")
            
            # FIXED: Direct write using Spark JDBC - exactly like notebook
            self.jcap_connector.write_table(
                df=df_transformed,
                table_name=self.main_table,
                schema=self.schema,
                mode="append"  # Table is already truncated
            )
            
            # Verify the load
            final_count = self.jcap_connector.get_table_count(self.main_table, self.schema)
            logger.info(f"📊 Final table count: {final_count:,}")
            
            if final_count == 0:
                raise RuntimeError("No rows loaded despite successful write operation!")
            
            if final_count != row_count:
                logger.warning(f"⚠️ Row count mismatch: Expected {row_count:,}, Got {final_count:,}")
            
            logger.info("✅ Data loaded to destination successfully")
            
        except Exception as e:
            logger.exception(f"❌ Destination load failed: {str(e)}")
            raise RuntimeError(f"Destination load failed: {str(e)}") from e
    
    def _validate_and_alert(self, previous_count: int, current_count: int) -> Dict[str, Any]:
        """Validate data variance and send alerts if needed."""
        try:
            logger.info("📊 Validating data variance")
            
            # Calculate variance
            variance = abs(current_count - previous_count)
            variance_percentage = (variance / previous_count) * 100 if previous_count > 0 else 0
            
            logger.info(f"📈 Previous: {previous_count:,}, Current: {current_count:,}")
            logger.info(f"📈 Variance: {variance:,} ({variance_percentage:.2f}%)")
            
            # Check threshold
            threshold_exceeded = variance_percentage >= self.settings.DATA_VARIANCE_THRESHOLD
            email_sent = False
            
            if threshold_exceeded:
                logger.warning(f"⚠️ Variance {variance_percentage:.2f}% exceeds threshold {self.settings.DATA_VARIANCE_THRESHOLD}%")
                
                email_sent = self.email_service.send_data_variance_alert(
                    variance_percentage=variance_percentage,
                    job_name="JCAP PA ETL",
                    previous_count=previous_count,
                    current_count=current_count
                )
                
                if email_sent:
                    logger.info("📧 Variance alert sent successfully")
                else:
                    logger.error("❌ Failed to send variance alert")
            else:
                logger.info(f"✅ Variance within acceptable threshold")
            
            return {
                "variance_percentage": variance_percentage,
                "threshold_exceeded": threshold_exceeded,
                "email_sent": email_sent
            }
            
        except Exception as e:
            logger.exception(f"❌ Validation failed: {str(e)}")
            raise RuntimeError(f"Validation failed: {str(e)}") from e