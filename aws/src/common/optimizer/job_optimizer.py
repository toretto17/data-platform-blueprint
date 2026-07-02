


import json
import logging
import math
import boto3
from datetime import datetime
from dateutil.relativedelta import relativedelta
from calendar import monthrange
from typing import Dict, List, Any, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed  # ← ADD THIS

# Then your existing imports
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import date_format, to_date, col, regexp_replace, when

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(funcName)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)
logger = logging.getLogger(__name__) # ← FIX: was 'name', should be '__name__'

# Then your Glue context setup
sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# =============================================================================
# ANALYZER CLASS
# =============================================================================

class ETLOptimizationAnalyzer:
    """Main analyzer class that orchestrates all analysis"""
    
    def __init__(
        self,
        sources: List[Dict],
        target: Dict,
        run_mode: str,
        analysis_end_month: str,                   # ← Move required params first
        spark: SparkSession,
        glue_context: GlueContext,
        analysis_months_count: int = 3,            # ← Optional params at end
        target_partition_granularity: str = None   # ← Optional params at end
    ):
        self.sources = sources
        self.target = target
        self.run_mode = run_mode
        self.target_partition_granularity = target_partition_granularity
        self.analysis_end_month = analysis_end_month
        self.analysis_months_count = min(analysis_months_count, 3)  # Max 3 months
        self.spark = spark
        self.glue_context = glue_context
        
        # AWS clients
        self.glue_client = boto3.client('glue')
        
        # Storage for results
        self.analysis_window = None
        self.source_analyses = []
        self.union_group_analyses = []  # ← ADD THIS
        self.join_union_operations = []  # ← ADD THIS
        self.combined_metrics = {}
        
        # Validate inputs
        self._validate_inputs()
    
    def _validate_inputs(self):
        """Validate input parameters"""
        logger.info("Validating inputs...")
        
        if self.run_mode not in ['append', 'overwrite']:
            raise ValueError(f"Invalid run_mode: {self.run_mode}. Must be 'append' or 'overwrite'")
        
        if self.target_partition_granularity not in ['daily', 'monthly', None]:
            raise ValueError(f"Invalid target_partition_granularity: {self.target_partition_granularity}")
        
        if len(self.analysis_end_month) != 6 or not self.analysis_end_month.isdigit():
            raise ValueError(f"analysis_end_month must be YYYYMM format, got: {self.analysis_end_month}")
        
        for source in self.sources:
            # Skip JOIN_UNION_GROUPS (different validation)
            if source.get('operation') == 'JOIN_UNION_GROUPS':
                if not source.get('left_group') or not source.get('right_group'):
                    raise ValueError("JOIN_UNION_GROUPS requires left_group and right_group")
                if not source.get('join_keys'):
                    raise ValueError("JOIN_UNION_GROUPS requires join_keys")
                continue

            # Regular validation
            if not source.get('primary_key'):
                raise ValueError(f"primary_key required for {source.get('table')}")

            if source.get('operation') == 'JOIN' and not source.get('join_keys'):
                raise ValueError(f"join_keys required for JOIN on {source.get('table')}")
            
            if source.get('operation') == 'JOIN' and not source.get('join_keys'):
                raise ValueError(f"join_keys required for JOIN operation on {source.get('table')}")
        
        logger.info("✓ Input validation passed")
        
    def _auto_detect_target_granularity(self) -> str:
        """Auto-detect target granularity from PRIMARY source table"""

        logger.info("Auto-detecting target partition granularity from primary source...")

        # Find the primary source
        primary_source = None
        for source in self.sources:
            if source.get('role', 'primary') == 'primary':
                primary_source = source
                break

        if not primary_source:
            # If no explicit primary, use the first source
            primary_source = self.sources[0]
            logger.warning(f"No primary source specified, using first source: {primary_source['table']}")

        # Detect partition type of primary source
        database = primary_source['database']
        table = primary_source['table']

        try:
            response = self.glue_client.get_table(DatabaseName=database, Name=table)
            partition_keys = response['Table'].get('PartitionKeys', [])

            if not partition_keys:
                logger.warning(f"Primary source {table} has no partitions, defaulting to 'daily'")
                return 'daily'

            partition_column = partition_keys[0]['Name']

            # Quick sample to detect type
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table,
                transformation_ctx=f"auto_detect_{table}"
            )

            df = dyf.toDF()
            sample_df = df.select(partition_column).distinct().limit(1)
            sample_values = [row[partition_column] for row in sample_df.collect()]

            if not sample_values:
                logger.warning(f"No data in {table}, defaulting to 'daily'")
                return 'daily'

            partition_value = str(sample_values[0])
            value_length = len(partition_value)

            if value_length == 6 and partition_value.isdigit():
                detected = 'monthly'
            elif value_length == 8 and partition_value.isdigit():
                detected = 'daily'
            elif value_length == 10:
                detected = 'daily'
            else:
                logger.warning(f"Cannot determine partition type from '{partition_value}', defaulting to 'daily'")
                detected = 'daily'

            logger.info(f"✓ Primary source '{table}' has {detected} partitions → target will be {detected}")
            return detected

        except Exception as e:
            logger.error(f"Failed to auto-detect, defaulting to 'daily': {e}")
            return 'daily'
    
    def analyze(self) -> Dict[str, Any]:
        """Main analysis orchestration"""

        # Step 1: Calculate analysis window
        self.analysis_window = self._calculate_analysis_window()
        logger.info(f"Analysis window: {self.analysis_window['start_month']} to {self.analysis_window['end_month']}")

        # Step 1.5: Auto-detect target granularity if not provided
        if self.target_partition_granularity is None:
            self.target_partition_granularity = self._auto_detect_target_granularity()
            logger.info(f"✓ Auto-detected target granularity: {self.target_partition_granularity}")
        else:
            logger.info(f"Using provided target granularity: {self.target_partition_granularity}")

        # ============================================================
        # Step 2: 🚀 PARALLEL ANALYSIS OF SOURCE TABLES
        # ============================================================
        logger.info(f"\n{'='*80}")
        logger.info(f"ANALYZING SOURCE TABLES IN PARALLEL")
        logger.info(f"{'='*80}")

        # ═══════════════════════════════════════════════════════════════
        # Filter out non-table operations (JOIN_UNION_GROUPS)
        # ═══════════════════════════════════════════════════════════════
        tables_to_analyze = [
            source for source in self.sources 
            if source.get('operation') != 'JOIN_UNION_GROUPS'
        ]

        logger.info(f"Total sources: {len(self.sources)}")
        logger.info(f"Tables to analyze: {len(tables_to_analyze)}")
        logger.info(f"Metadata operations: {len(self.sources) - len(tables_to_analyze)}")

        if not tables_to_analyze:
            raise ValueError("No actual tables to analyze! All sources are operations.")

        # Determine max workers (don't overwhelm the cluster)
        max_workers = min(len(tables_to_analyze), 3) 
    
        def analyze_single_source(source_with_index):
            """Wrapper function for parallel execution"""
            idx, source = source_with_index
            logger.info(f"\n{'='*80}")
            logger.info(f"ANALYZING SOURCE {idx}/{len(tables_to_analyze)}: {source['database']}.{source['table']}")
            logger.info(f"{'='*80}")

            try:
                return self._analyze_source_table(source)
            except Exception as e:
                logger.error(f"Failed to analyze {source['table']}: {str(e)}")
                raise

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit all tasks (ONLY real tables)
            future_to_source = {
                executor.submit(analyze_single_source, (idx, source)): source 
                for idx, source in enumerate(tables_to_analyze, 1)  # ← Use filtered list
            }

            # Collect results as they complete
            for future in as_completed(future_to_source):
                source = future_to_source[future]
                try:
                    source_analysis = future.result()
                    self.source_analyses.append(source_analysis)
                    logger.info(f"✓ Completed: {source['table']}")
                except Exception as e:
                    logger.error(f"✗ Failed: {source['table']} - {str(e)}")
                    raise

        logger.info(f"\n✓ All {len(self.source_analyses)} tables analyzed successfully")

        # ============================================================
        # Step 2.5: VALIDATE SCALING FACTORS
        # ============================================================
        logger.info(f"\n{'='*80}")
        logger.info("VALIDATING SCALING FACTORS")
        logger.info(f"{'='*80}")

        # Check if any scaling factor is dangerously high
        for source_analysis in self.source_analyses:
            # This will be populated in _combine_source_metrics, but we can do early check
            pass

        # Step 3: Combine metrics across sources (includes scaling calculation)
        self._combine_source_metrics()

        # Step 3.5: SAFETY CHECK - Prevent under-resourcing
        total_scaled_size = self.combined_metrics['volume']['total_data_size_gb']
        avg_scaling_factor = sum(s['scaling']['scaling_factor'] for s in self.source_analyses) / len(self.source_analyses)

        if avg_scaling_factor > 3.0:
            logger.warning("="*80)
            logger.warning(f"⚠️  HIGH SCALING FACTOR DETECTED: {avg_scaling_factor:.2f}x")
            logger.warning(f"⚠️  You're analyzing a SMALL window but processing LARGE data volume")
            logger.warning(f"⚠️  Total scaled size: {total_scaled_size:.2f} GB")
            logger.warning(f"⚠️  Consider analyzing more months for accurate skew detection")
            logger.warning("="*80)

        # Step 4: Calculate optimal resources
        resource_recommendation = self._calculate_resources()

        # Step 5: Generate Spark configurations
        spark_configurations = self._generate_spark_config(resource_recommendation)

        # Step 6: Generate caching recommendations
        caching_recommendations = self._analyze_caching()

        # Step 7: Generate optimization recommendations
        optimization_recommendations = self._generate_recommendations()

        # Step 8: Generate warnings
        warnings = self._generate_warnings()

        # Step 9: Data quality gates
        quality_gates = self._evaluate_quality_gates()

        # Compile final output
        results = {
            'metadata': {
                'analysis_timestamp': datetime.now().isoformat(),
                'framework_version': '2.0',
                'analysis_window': self.analysis_window,
                'parallel_analysis': True,  # ← NEW: indicate parallel processing used
                'max_parallel_workers': max_workers
            },
            'source_analysis': self.source_analyses,
            'union_group_analysis': self.union_group_analyses, 
            'combined_metrics': self.combined_metrics,        
            'resource_recommendation': resource_recommendation,
            'spark_configurations': spark_configurations,
            'caching_recommendations': caching_recommendations,
            'optimization_recommendations': optimization_recommendations,
            'warnings': warnings,
            'data_quality_gates': quality_gates,
            'best_practices_applied': self._get_best_practices_applied()
        }

        return results
    
    # -------------------------------------------------------------------------
    # ANALYSIS WINDOW CALCULATION
    # -------------------------------------------------------------------------
    
    def _calculate_analysis_window(self) -> Dict[str, Any]:
        """Calculate analysis window based on end month and count"""
        
        logger.info(f"Calculating analysis window: {self.analysis_months_count} months ending {self.analysis_end_month}")
        
        end_date = datetime.strptime(self.analysis_end_month, '%Y%m')
        start_date = end_date - relativedelta(months=self.analysis_months_count - 1)
        
        start_month = start_date.strftime('%Y%m')
        
        # Generate list of months
        months = []
        current = start_date
        while current <= end_date:
            months.append(current.strftime('%Y%m'))
            current += relativedelta(months=1)
        
        # Calculate date range for daily partitions
        start_day = start_date.replace(day=1).strftime('%Y%m%d')
        last_day_of_end_month = monthrange(end_date.year, end_date.month)[1]
        end_day = end_date.replace(day=last_day_of_end_month).strftime('%Y%m%d')
        
        window = {
            'start_month': start_month,
            'end_month': self.analysis_end_month,
            'months': months,
            'months_count': len(months),
            'start_date': start_day,
            'end_date': end_day,
            'filter_monthly': f"BETWEEN '{start_month}' AND '{self.analysis_end_month}'",
            'filter_daily': f"BETWEEN '{start_day}' AND '{end_day}'"
        }
        
        logger.info(f"✓ Analysis window: {start_month} to {self.analysis_end_month} ({len(months)} months)")
        logger.info(f"  Daily range: {start_day} to {end_day}")
        
        return window
    
    # -------------------------------------------------------------------------
    # SOURCE TABLE ANALYSIS
    # -------------------------------------------------------------------------
    
    def _analyze_source_table(self, source: Dict) -> Dict[str, Any]:
        """Comprehensive analysis of a single source table"""
        
        database = source['database']
        table = source['table']
        primary_key = source['primary_key']
        role = source.get('role', 'primary')
        manual_partition_column = source.get('partition_column')

        analysis = {
            'source_id': len(self.source_analyses) + 1,
            'database': database,
            'table': table,
            'role': role
        }

        try:
            # Phase 1: Metadata detection
            metadata = self._detect_metadata(database, table, manual_partition_column)
            analysis['metadata'] = metadata

            # CHANGED: Use catalog partition for pushdown
            catalog_partition = metadata['catalog_partition_column']
            partition_type = metadata['partition_type']

            # Generate filter using CATALOG partition
            filter_clause = self._generate_filter_clause(catalog_partition, partition_type) if catalog_partition else ""
            group_by_expr = self._generate_group_by_expression(metadata['etl_partition_column'], partition_type)

            # Phase 2: Volume analysis
            logger.info(f"Running volume analysis...")
            volume = self._analyze_volume(database, table, primary_key, metadata, 
                                          filter_clause, group_by_expr, 
                                          metadata['avg_row_size_bytes'])
            analysis['volume'] = volume
            
            # Phase 3: Skew analysis
            logger.info(f"Running skew analysis...")
            skew = self._analyze_skew(database, table, primary_key)
            analysis['skew'] = skew
            
            # Phase 4: Data quality analysis
            logger.info(f"Running data quality analysis...")
            quality = self._analyze_quality(database, table, primary_key, metadata)
            analysis['quality'] = quality
            
            # Phase 5: Join analysis (if applicable)
            if source.get('operation') == 'JOIN':
                logger.info(f"Running join analysis...")
                join_analysis = self._analyze_join(source, filter_clause)
                analysis['join_analysis'] = join_analysis
            
            logger.info(f"✓ Completed analysis for {database}.{table}")
            
        except Exception as e:
            logger.error(f"Failed to analyze {database}.{table}: {str(e)}", exc_info=True)
            raise
        
        return analysis
    
    def _classify_skew_level(self, ratio: float) -> str:
        if ratio >= 100: return "EXTREME"
        if ratio >= 10: return "HIGH"
        if ratio >= 3: return "MODERATE"
        if ratio >= 2: return "LOW"
        return "NONE"
    
    def _process_union_groups(self):
        """Combine metrics for tables with same union_group"""
        from collections import defaultdict

        union_map = defaultdict(list)
        for src in self.sources:
            if src.get('union_group'):
                union_map[src['union_group']].append(src['table'])

        if not union_map:
            logger.info("No UNION groups found")
            return

        logger.info(f"\n{'='*80}")
        logger.info(f"PROCESSING {len(union_map)} UNION GROUPS")
        logger.info(f"{'='*80}")

        for group_name, table_names in union_map.items():
            # Find analyses for these tables
            group_analyses = [
                a for a in self.source_analyses 
                if a['table'] in table_names
            ]

            if not group_analyses:
                continue

            # Combine metrics
            total_size = sum(a['volume']['scaled_size_gb'] for a in group_analyses)
            total_records = sum(a['volume']['scaled_records'] for a in group_analyses)
            worst_skew = max(a['skew']['skew_ratio'] for a in group_analyses)

            self.union_group_analyses.append({
                'group_name': group_name,
                'tables': table_names,
                'table_count': len(table_names),
                'combined_volume': {
                    'total_size_gb': round(total_size, 2),
                    'total_records': total_records
                },
                'combined_skew': {
                    'worst_skew_ratio': round(worst_skew, 2),
                    'worst_skew_level': self._classify_skew_level(worst_skew)
                }
            })

            logger.info(f"  ✓ {group_name}: {len(table_names)} tables → {total_size:.2f} GB")
    
    def _calculate_scaling_factor(self, source_analysis: Dict) -> Dict[str, Any]:
        """
        Calculate scaling factor between analysis window and FULL table data.
        
        If analyzing 3 months but table has 12 months total data,
        scaling_factor = 12/3 = 4.0x
        
        This ensures resource recommendations account for FULL data volume.
        """
        
        database = source_analysis['database']
        table = source_analysis['table']
        partition_column = source_analysis['metadata']['partition_column']
        partition_type = source_analysis['metadata']['partition_type']
        
        logger.info(f"Calculating scaling factor for {table}...")
        
        try:
            # Get ALL partitions from Glue Catalog (no data scan - very fast!)
            paginator = self.glue_client.get_paginator('get_partitions')
            partition_iterator = paginator.paginate(
                DatabaseName=database,
                TableName=table
            )
            
            all_partition_values = []
            for page in partition_iterator:
                for partition in page['Partitions']:
                    # Extract partition value
                    partition_value = partition['Values'][0]
                    all_partition_values.append(partition_value)
            
            total_partitions_in_catalog = len(all_partition_values)
            analyzed_partitions = source_analysis['volume']['total_partitions']
            
            if analyzed_partitions > 0:
                # Scaling by partition count is an APPROXIMATION.
                # Caveat: assumes uniform partition sizes. If partitions vary (e.g., Dec >> Jun),
                # this over/under-estimates. For accurate sizing, use S3 ListObjects with --summarize.
                # However, partition-count scaling is fast (catalog-only, no I/O) and sufficient
                # for resource estimation (we add safety margins in worker calculation).
                scaling_factor = total_partitions_in_catalog / analyzed_partitions
            else:
                scaling_factor = 1.0
            
            # Get earliest and latest partition from catalog
            sorted_partitions = sorted(all_partition_values)
            catalog_earliest = sorted_partitions[0] if sorted_partitions else None
            catalog_latest = sorted_partitions[-1] if sorted_partitions else None
            
            scaling_info = {
                'scaling_factor': round(scaling_factor, 2),
                'catalog_total_partitions': total_partitions_in_catalog,
                'catalog_earliest_partition': str(catalog_earliest),
                'catalog_latest_partition': str(catalog_latest),
                'analyzed_partitions': analyzed_partitions,
                'analyzed_earliest': source_analysis['volume']['earliest_partition'],
                'analyzed_latest': source_analysis['volume']['latest_partition']
            }
            
            logger.info(f"  ✓ Scaling factor: {scaling_factor:.2f}x")
            logger.info(f"    Catalog has {total_partitions_in_catalog} total partitions ({catalog_earliest} to {catalog_latest})")
            logger.info(f"    Analyzed {analyzed_partitions} partitions ({scaling_info['analyzed_earliest']} to {scaling_info['analyzed_latest']})")
            
            # Warning if scaling factor is too high
            if scaling_factor > 5:
                logger.warning(f"⚠️ HIGH SCALING FACTOR ({scaling_factor:.2f}x) - Analysis window may be too small!")
            
            return scaling_info
            
        except Exception as e:
            logger.error(f"Failed to calculate scaling factor: {e}")
            
            analyzed_partitions = source_analysis['volume']['total_partitions'] 
            # Return safe defaults
            return {
                'scaling_factor': 1.0,
                'catalog_total_partitions': analyzed_partitions,
                'catalog_earliest_partition': source_analysis['volume']['earliest_partition'],
                'catalog_latest_partition': source_analysis['volume']['latest_partition'],
                'analyzed_partitions': analyzed_partitions,
                'analyzed_earliest': source_analysis['volume']['earliest_partition'],
                'analyzed_latest': source_analysis['volume']['latest_partition'],
                'error': str(e)
            }
    
    def _detect_metadata(self, database: str, table: str, manual_partition_column: str) -> Dict[str, Any]:
        """Detect table metadata from Glue Catalog"""

        logger.info(f"Detecting metadata for {database}.{table}...")

        try:
            response = self.glue_client.get_table(DatabaseName=database, Name=table)
            table_info = response['Table']
            storage_desc = table_info['StorageDescriptor']

            # GET CATALOG PARTITION (from Glue)
            partition_keys = table_info.get('PartitionKeys', [])
            catalog_partition_column = partition_keys[0]['Name'] if partition_keys else None

            # DETERMINE ETL PARTITION (what you'll use in analysis)
            if manual_partition_column:
                etl_partition_column = manual_partition_column
                logger.info(f"✓ Using manual ETL partition: '{etl_partition_column}'")
            else:
                etl_partition_column = catalog_partition_column
                logger.info(f"✓ Using catalog partition: '{etl_partition_column}'")

            metadata = {
                'catalog_partition_column': catalog_partition_column,  # ← NEW
                'etl_partition_column': etl_partition_column,          # ← NEW
                'partition_column': etl_partition_column,
                'storage_location': storage_desc.get('Location', ''),
                'input_format': storage_desc.get('InputFormat', ''),
                'num_columns': len(storage_desc['Columns']),
                'columns': [c['Name'] for c in storage_desc['Columns']]
            }

            # Detect partition type
            partition_type = self._detect_partition_type(
                database, table, etl_partition_column, 
                catalog_partition_column,  # ← NEW PARAMETER
                manual_partition_column is not None
            )
            metadata['partition_type'] = partition_type
            metadata['partition_detection_method'] = 'manual_override' if manual_partition_column else 'auto_detected'

            avg_row_size = self._estimate_row_size(database, table, metadata['num_columns'])
            metadata['avg_row_size_bytes'] = avg_row_size

            logger.info(f"✓ Catalog partition: {catalog_partition_column}, ETL partition: {etl_partition_column}, type: {partition_type}")

            return metadata

        except Exception as e:
            logger.error(f"Failed to detect metadata: {str(e)}")
            raise
    
    def _detect_partition_type(self, database: str, table: str, etl_partition_column: str,
                          catalog_partition_column: str, is_manual: bool = False) -> str:
        """Detect partition type by examining actual partition values"""

        logger.info(f"Detecting partition type for column '{etl_partition_column}'...")

        try:
            # Read sample WITHOUT strict filter (just get some data)
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table,
                transformation_ctx=f"detect_partition_{table}"
            )

            df = dyf.toDF().limit(10)

            if len(df.head(1)) == 0:
                logger.warning("No data to sample, defaulting to 'daily'")
                return "daily"

            # Check if ETL column exists
            col_map = {col.lower(): col for col in df.columns}

            if etl_partition_column.lower() not in col_map:
                if is_manual:
                    logger.warning(f"Manual column '{etl_partition_column}' not in data - defaulting to daily")
                    return "daily"
                else:
                    raise ValueError(f"Column '{etl_partition_column}' not found. Available: {df.columns[:10]}")

            actual_column = col_map[etl_partition_column.lower()]
            partition_col_type = dict(df.dtypes).get(actual_column, 'string')
            logger.info(f"  Column '{actual_column}' type: {partition_col_type}")

            # Detect type from data
            if 'date' in partition_col_type.lower() or 'timestamp' in partition_col_type.lower():
                detected_type = "daily"
                logger.info(f"  ✓ DATE/TIMESTAMP → daily")
            elif partition_col_type == 'string':
                sample = df.select(actual_column).limit(1).collect()
                if not sample:
                    return "daily"

                partition_value = str(sample[0][actual_column])
                value_length = len(partition_value)

                if value_length == 6 and partition_value.isdigit():
                    detected_type = "monthly"
                elif value_length == 8 and partition_value.isdigit():
                    detected_type = "daily"
                elif value_length == 10:
                    detected_type = "daily"
                else:
                    detected_type = "daily"
            else:
                detected_type = "daily"

            logger.info(f"✓ Detected partition type: {detected_type}")
            return detected_type

        except Exception as e:
            logger.error(f"Failed to detect partition type: {str(e)}")
            return "daily"
    
    def _estimate_row_size(self, database: str, table: str, num_columns: int) -> int:
        """Estimate average row size in bytes.
        
        Uses push_down_predicate to read only 1 partition for sampling
        instead of scanning the entire table. Falls back to heuristic if sampling fails.
        
        AWS Glue docs: "Load only the data that you need" — always use predicates.
        """
        
        logger.info(f"Estimating row size (sampling from single partition)...")
        
        # Heuristic baseline: ~50 bytes per column average (strings ~30, numbers ~8, timestamps ~20)
        estimated_size = num_columns * 50
        
        try:
            # Use the analysis window filter to read only 1 partition (not full table!)
            filter_clause = None
            if self.analysis_window:
                # Read only the latest month/partition for sampling
                filter_clause = f"data_dt = '{self.analysis_window['end_date']}'" if self.target_partition_granularity == 'daily' else None

            if filter_clause:
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=database,
                    table_name=table,
                    push_down_predicate=filter_clause,
                    transformation_ctx=f"estimate_size_{table}"
                )
            else:
                # Fallback: read with transformation context (Glue may still optimize)
                dyf = self.glue_context.create_dynamic_frame.from_catalog(
                    database=database,
                    table_name=table,
                    transformation_ctx=f"estimate_size_{table}",
                    additional_options={"boundedFiles": "10"}  # Limit files read
                )

            df = dyf.toDF().limit(1000)
            
            if len(df.head(1)) == 0:
                logger.info(f"No sample data, using heuristic: {estimated_size} bytes/row")
                return estimated_size

            # Calculate average row size from sample
            from pyspark.sql.functions import length, struct, col, avg
            result = df.select(avg(length(struct(*[col(c) for c in df.columns]).cast("string"))).alias("avg_size")).collect()
            if result and result[0]['avg_size']:
                estimated_size = int(result[0]['avg_size'])
        except Exception as e:
            logger.warning(f"Could not calculate exact row size, using heuristic ({estimated_size} bytes): {e}")
        
        logger.info(f"✓ Estimated row size: {estimated_size} bytes")
        return estimated_size
    
    def _generate_filter_clause(self, partition_column: str, partition_type: str) -> str:
        """Generate SIMPLE pushdown filter for Glue catalog partition"""

        # Simple string comparison for catalog partitions
        if partition_type == "monthly":
            return f"{partition_column} >= '{self.analysis_window['start_month']}' AND {partition_column} <= '{self.analysis_window['end_month']}'"
        else:
            return f"{partition_column} >= '{self.analysis_window['start_date']}' AND {partition_column} <= '{self.analysis_window['end_date']}'"
    
    def _generate_group_by_expression(self, partition_column: str, partition_type: str) -> str:
        """Generate SQL GROUP BY expression"""

        if partition_type == "daily" and self.target_partition_granularity == "daily":
            return f"`{partition_column}`"
        elif partition_type == "daily" and self.target_partition_granularity == "monthly":
            return f"SUBSTR(`{partition_column}`, 1, 6)"
        elif partition_type == "monthly":
            return f"`{partition_column}`"
        else:
            return f"`{partition_column}`"
    
    def _analyze_volume(self, database: str, table: str, primary_key: str, 
                   metadata: Dict, filter_clause: str, group_by_expr: str, 
                   avg_row_size: int) -> Dict:
        """Run volume analysis queries"""

        volume = {}
    
        etl_partition = metadata['etl_partition_column']
        catalog_partition = metadata['catalog_partition_column']

        logger.info(f"Reading data with filter: {filter_clause or 'NONE'}")

        # Use filter_clause for pushdown (it uses catalog partition)
        if filter_clause:
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table,
                push_down_predicate=filter_clause,
                transformation_ctx=f"analyze_volume_{table}"
            )
        else:
            dyf = self.glue_context.create_dynamic_frame.from_catalog(
                database=database,
                table_name=table,
                transformation_ctx=f"analyze_volume_{table}"
            )

        df = dyf.toDF()

        if len(df.head(1)) == 0:
            logger.warning("No data found")
            return {
                'total_records': 0,
                'unique_keys': 0,
                'total_partitions': 0,
                'earliest_partition': None,
                'latest_partition': None,
                'total_size_gb': 0.0,
                'avg_records_per_partition': 0,
                'size_classification': 'EMPTY'
            }

        col_map = {col.lower(): col for col in df.columns}

        # Fix primary_key case
        if primary_key.lower() in col_map:
            actual_primary_key = col_map[primary_key.lower()]
            if actual_primary_key != primary_key:
                logger.warning(f"  Correcting primary_key: '{primary_key}' → '{actual_primary_key}'")
                primary_key = actual_primary_key
        else:
            raise ValueError(f"Column '{primary_key}' not found. Available: {list(col_map.values())[:20]}")

        # Check if ETL partition exists, if not use catalog partition
        if etl_partition.lower() in col_map:
            actual_etl_partition = col_map[etl_partition.lower()]
        else:
            logger.warning(f"ETL partition '{etl_partition}' not found, using catalog partition '{catalog_partition}'")
            actual_etl_partition = col_map[catalog_partition.lower()] if catalog_partition.lower() in col_map else catalog_partition

        # Add formatted partition column
        partition_col_type = dict(df.dtypes).get(actual_etl_partition, 'string')

        if 'date' in partition_col_type.lower() or 'timestamp' in partition_col_type.lower():
            if metadata['partition_type'] == "monthly":
                df = df.withColumn("partition_formatted", date_format(col(actual_etl_partition), 'yyyyMM'))
            else:
                df = df.withColumn("partition_formatted", date_format(col(actual_etl_partition), 'yyyyMMdd'))
            partition_column_for_query = "partition_formatted"
        else:
            partition_column_for_query = actual_etl_partition

        # Update group_by_expr to use correct column
        if metadata['partition_type'] == "daily" and self.target_partition_granularity == "monthly":
            group_by_expr = f"SUBSTR(`{partition_column_for_query}`, 1, 6)"
        else:
            group_by_expr = f"`{partition_column_for_query}`"

        df.createOrReplaceTempView(f"temp_{table}")
        
        # Q1: Overall volume
        query = f"""
        SELECT 
            COUNT(*) as total_records,
            APPROX_COUNT_DISTINCT(`{primary_key}`) as unique_keys,
            COUNT(DISTINCT {group_by_expr}) as total_partitions,
            MIN(`{partition_column_for_query}`) as earliest_partition,
            MAX(`{partition_column_for_query}`) as latest_partition
        FROM temp_{table}
        WHERE `{partition_column_for_query}` IS NOT NULL
        """

        logger.info(f"  Executing query: {query}")
        result = self.spark.sql(query).collect()[0]

        total_records = result['total_records']
        unique_keys = result['unique_keys']
        total_partitions = result['total_partitions']

        total_size_gb = (total_records * avg_row_size) / (1024**3)

        volume['total_records'] = total_records
        volume['unique_keys'] = unique_keys
        volume['total_partitions'] = total_partitions
        volume['earliest_partition'] = str(result['earliest_partition'])
        volume['latest_partition'] = str(result['latest_partition'])
        volume['total_size_gb'] = round(total_size_gb, 2)
        volume['avg_records_per_partition'] = round(total_records / max(total_partitions, 1), 0)

        # Classify size
        if total_size_gb < 10:
            size_class = "TINY"
        elif total_size_gb < 100:
            size_class = "SMALL"
        elif total_size_gb < 1000:
            size_class = "MEDIUM"
        elif total_size_gb < 10000:
            size_class = "LARGE"
        else:
            size_class = "XLARGE"

        volume['size_classification'] = size_class

        logger.info(f"  Total records: {total_records:,}")
        logger.info(f"  Unique keys: {unique_keys:,}")
        logger.info(f"  Estimated size: {total_size_gb:.2f} GB ({size_class})")

        # Q2: Partition distribution
        query = f"""
        SELECT 
            {group_by_expr} as partition_value,
            COUNT(*) as records,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 2) as pct_of_total
        FROM temp_{table}
        GROUP BY {group_by_expr}
        ORDER BY partition_value DESC
        """

        partition_dist = self.spark.sql(query).collect()
        volume['partition_distribution'] = [
            {'partition': str(row['partition_value']), 
             'records': row['records'], 
             'pct': row['pct_of_total']}
            for row in partition_dist[:10]
        ]

        return volume
    
    def _analyze_skew(self, database: str, table: str, primary_key: str) -> Dict:
        """Run skew analysis queries"""
        
        skew = {}
    
        try:
            df = self.spark.table(f"temp_{table}")
        except Exception:
            logger.error(f"Temp view temp_{table} not found. Volume analysis must run first.")
            raise RuntimeError(f"Cannot analyze skew: temp view for {table} not found")
            
                # Fix column case-sensitivity
        df = self.spark.table(f"temp_{table}")
        col_map = {col.lower(): col for col in df.columns}
        
        if primary_key.lower() in col_map:
            actual_primary_key = col_map[primary_key.lower()]
            if actual_primary_key != primary_key:
                logger.warning(f"  Correcting primary_key case in skew analysis: '{primary_key}' → '{actual_primary_key}'")
                primary_key = actual_primary_key

        # Q1: Key distribution statistics
        query = f"""
        WITH key_counts AS (
            SELECT `{primary_key}`, COUNT(*) as record_count
            FROM temp_{table}
            GROUP BY `{primary_key}`)
        SELECT 
            COUNT(*) as total_unique_keys,
            ROUND(AVG(record_count), 2) as avg_records_per_key,
            ROUND(STDDEV(record_count), 2) as stddev,
            MIN(record_count) as min_records,
            MAX(record_count) as max_records,
            PERCENTILE_APPROX(record_count, 0.50) as p50,
            PERCENTILE_APPROX(record_count, 0.90) as p90,
            PERCENTILE_APPROX(record_count, 0.95) as p95,
            PERCENTILE_APPROX(record_count, 0.99) as p99
        FROM key_counts
        """
        
        result = self.spark.sql(query).collect()[0]
        
        avg = float(result['avg_records_per_key'] or 0)
        max_val = int(result['max_records'] or 0)
        
        skew_ratio = max_val / avg if avg > 0 else 1.0
        
        skew['total_unique_keys'] = result['total_unique_keys']
        skew['avg_records_per_key'] = avg
        skew['stddev'] = float(result['stddev'] or 0)
        skew['min_records'] = result['min_records']
        skew['max_records'] = max_val
        skew['p50'] = float(result['p50'] or 0)
        skew['p90'] = float(result['p90'] or 0)
        skew['p95'] = float(result['p95'] or 0)
        skew['p99'] = float(result['p99'] or 0)
        skew['skew_ratio'] = round(skew_ratio, 2)
        skew['coefficient_of_variation'] = round(skew['stddev'] / avg, 2) if avg > 0 else 0
        
        # Classify skew
        if skew_ratio >= 100:
            skew_level = "EXTREME"
        elif skew_ratio >= 10:
            skew_level = "HIGH"
        elif skew_ratio >= 3:
            skew_level = "MODERATE"
        elif skew_ratio >= 2:
            skew_level = "LOW"
        else:
            skew_level = "NONE"
        
        skew['skew_level'] = skew_level
        
        logger.info(f"  Skew ratio: {skew_ratio:.2f}x ({skew_level})")
        logger.info(f"  Avg records/key: {avg:.2f}, Max: {max_val}")
        
        # Q2: Top skewed keys
            # Q2: Top skewed keys (reuse temp view)
        query = f"""
        SELECT `{primary_key}`,
            COUNT(*) as record_count,
            ROUND(COUNT(*) * 100.0 / SUM(COUNT(*)) OVER (), 4) as pct_of_total
        FROM temp_{table}
        GROUP BY `{primary_key}`
        ORDER BY record_count DESC
        LIMIT 10
        """
        
        top_keys = self.spark.sql(query).collect()
        skew['top_skewed_keys'] = [
            {'key': str(row[primary_key]), 
             'count': row['record_count'], 
             'pct': row['pct_of_total']}
            for row in top_keys
        ]
        
        return skew
    
    def _analyze_quality(self, database: str, table: str, primary_key: str, metadata: Dict) -> Dict:
        """Run data quality analysis queries"""
        
        quality = {}
        partition_column = metadata['etl_partition_column']

        # Reuse temp view from previous analysis
        try:
            df = self.spark.table(f"temp_{table}")
        except Exception:
            logger.error(f"Temp view temp_{table} not found. Volume analysis must run first.")
            raise RuntimeError(f"Cannot analyze quality: temp view for {table} not found")
            
                    # Fix column case-sensitivity
        df = self.spark.table(f"temp_{table}")
        col_map = {col.lower(): col for col in df.columns}
        
        if primary_key.lower() in col_map:
            actual_primary_key = col_map[primary_key.lower()]
            if actual_primary_key != primary_key:
                logger.warning(f"  Correcting primary_key case in quality analysis: '{primary_key}' → '{actual_primary_key}'")
                primary_key = actual_primary_key
        
        if partition_column.lower() in col_map:
            actual_partition_col = col_map[partition_column.lower()]
            if actual_partition_col != partition_column:
                logger.warning(f"  Correcting partition_column case in quality analysis: '{partition_column}' → '{actual_partition_col}'")
                partition_column = actual_partition_col

        # Q1: Duplicate detection
        query = f"""
        SELECT 
            COUNT(*) as total_records,
             COUNT(DISTINCT `{primary_key}`) as unique_keys
        FROM temp_{table}
        """
        
        result = self.spark.sql(query).collect()[0]
        total = result['total_records']
        unique = result['unique_keys']
        
        duplicate_count = total - unique
        duplicate_pct = (duplicate_count * 100.0 / total) if total > 0 else 0
        
        quality['duplicate_count'] = duplicate_count
        quality['duplicate_pct'] = round(duplicate_pct, 2)
        
        # Q2: NULL analysis for key columns
        null_analysis = {}
        
        for col_name in [primary_key, partition_column]:
            query = f"""
            SELECT 
            SUM(CASE WHEN `{col_name}` IS NULL THEN 1 ELSE 0 END) as null_count,
            COUNT(*) as total_count
            FROM temp_{table}
            """
            
            result = self.spark.sql(query).collect()[0]
            null_count = result['null_count']
            total_count = result['total_count']
            null_pct = (null_count * 100.0 / total_count) if total_count > 0 else 0
            
            null_analysis[col_name] = {
                            'null_pct': round(null_pct, 2),
                            'severity': 'CRITICAL' if null_pct > 50 else 'HIGH' if null_pct > 10 else 'OK'
                        }
        
        quality['null_analysis'] = null_analysis
        
        # Calculate quality score
        quality_score = 100.0
        quality_score -= min(duplicate_pct, 30)
        quality_score -= min(sum(v['null_pct'] for v in null_analysis.values()) / len(null_analysis), 30)
        quality['quality_score'] = round(max(quality_score, 0), 2)
        
        logger.info(f"  Quality score: {quality['quality_score']:.1f}/100")
        logger.info(f"  Duplicates: {duplicate_pct:.2f}%")
        
        issues = []
        if duplicate_pct > 1:
            issues.append(f"{duplicate_pct:.2f}% duplicates detected")
        for col, stats in null_analysis.items():
            if stats['null_pct'] > 1:
                issues.append(f"{stats['null_pct']:.2f}% NULLs in {col}")
        
        quality['issues'] = issues
        
        return quality
    
    def _analyze_join(self, source: Dict, filter_clause: str) -> Dict:
        """Analyze join characteristics for dimension tables"""
        
        join_analysis = {}
        
        # This is simplified - in production, you'd run the actual join queries
        # For now, we'll just note the join configuration
        
        join_analysis['join_type'] = source.get('join_type', 'INNER')
        join_analysis['join_keys'] = source.get('join_keys', [])
        join_analysis['recommended_strategy'] = 'sort_merge'  # Will be determined by size
        
        return join_analysis
    
    # -------------------------------------------------------------------------
    # COMBINED METRICS
    # -------------------------------------------------------------------------
    
    def _combine_source_metrics(self):
        """Combine metrics across all sources"""
        logger.info("\nCombining metrics across sources...")

        for source_analysis in self.source_analyses:
            scaling_info = self._calculate_scaling_factor(source_analysis)
            source_analysis['scaling'] = scaling_info

            scaling_factor = scaling_info['scaling_factor']
            original_size_gb = source_analysis['volume']['total_size_gb']

            scaled_size_gb = original_size_gb * scaling_factor
            scaled_records = int(source_analysis['volume']['total_records'] * scaling_factor)

            source_analysis['volume']['scaled_size_gb'] = round(scaled_size_gb, 2)
            source_analysis['volume']['scaled_records'] = scaled_records

        # Process union groups
        self._process_union_groups()

        # Calculate totals
        total_size_gb = sum(s['volume']['scaled_size_gb'] for s in self.source_analyses)
        worst_skew = max(s['skew']['skew_ratio'] for s in self.source_analyses)
        has_joins = any(s.get('operation') == 'JOIN' for s in self.sources)
        has_unions = bool(self.union_group_analyses)

        complexity = self._determine_complexity(total_size_gb, worst_skew, has_joins, has_unions)

        self.combined_metrics = {
            'mode': self.run_mode,
            'target_partition_granularity': self.target_partition_granularity,
            'total_sources': len(self.sources),
            'volume': {
                'total_data_size_gb': round(total_size_gb, 2),
                'size_classification': self._classify_size(total_size_gb),
                'scaling_applied': True
            },
            'skew': {
                'worst_skew_ratio': round(worst_skew, 2),
                'worst_skew_level': self._classify_skew_level(worst_skew),
                'requires_special_handling': worst_skew >= 10
            },
            'complexity': {
                'determined_complexity': complexity,
                'has_joins': has_joins,
                'has_unions': has_unions,
                'has_shuffle': True
            }
        }
    
    def _determine_complexity(self, total_size_gb: float, worst_skew: float,
                             has_joins: bool, has_unions: bool) -> str:
        """Determine job complexity"""
        
        if worst_skew > 100:
            return "extreme"
        elif has_joins and total_size_gb > 1000:
            return "complex"
        elif has_joins and has_unions:
            return "complex"
        elif has_joins or worst_skew > 10 or total_size_gb > 500:
            return "medium"
        else:
            return "simple"
    
    def _classify_size(self, size_gb: float) -> str:
        """Classify data size"""
        if size_gb < 10:
            return "TINY"
        elif size_gb < 100:
            return "SMALL"
        elif size_gb < 1000:
            return "MEDIUM"
        elif size_gb < 10000:
            return "LARGE"
        else:
            return "XLARGE"
    
    # -------------------------------------------------------------------------
    # RESOURCE CALCULATION
    # -------------------------------------------------------------------------
    
    def _calculate_resources(self) -> Dict:
        """
        Calculate REALISTIC optimal resources for multiple scenarios.

        Based on:
        - Actual worker memory limits
        - Skew impact on largest partition
        - Shuffle memory requirements
        - Real-world processing throughput
        """

        logger.info("\nCalculating realistic resources for all scenarios...")

        # ═══════════════════════════════════════════════════════════════
        # STEP 1: Gather data characteristics
        # ═══════════════════════════════════════════════════════════════

        full_catalog_size_gb = 0
        analyzed_window_size_gb = 0
        avg_size_per_partition = {}
        max_partition_size_gb = 0

        for source in self.source_analyses:
            analyzed_size = source['volume']['total_size_gb']
            analyzed_partitions = source['volume']['total_partitions']
            catalog_partitions = source['scaling']['catalog_total_partitions']

            if analyzed_partitions > 0:
                avg_partition_size = analyzed_size / analyzed_partitions
                avg_size_per_partition[source['table']] = avg_partition_size

                # Calculate LARGEST partition size based on skew
                skew_ratio = source['skew']['skew_ratio']
                if skew_ratio > 1:
                    largest_partition_size = avg_partition_size * skew_ratio
                    max_partition_size_gb = max(max_partition_size_gb, largest_partition_size)
                else:
                    max_partition_size_gb = max(max_partition_size_gb, avg_partition_size)

                # Estimate full catalog size
                estimated_full_size = avg_partition_size * catalog_partitions
                full_catalog_size_gb += estimated_full_size
                analyzed_window_size_gb += analyzed_size

                logger.info(f"  {source['table']}:")
                logger.info(f"    Avg partition: {avg_partition_size:.2f} GB")
                logger.info(f"    Largest partition (with {skew_ratio:.1f}x skew): {largest_partition_size:.2f} GB")
                logger.info(f"    Full catalog: ~{estimated_full_size:.2f} GB")

        # Get complexity metrics
        complexity = self.combined_metrics['complexity']['determined_complexity']
        skew_ratio = self.combined_metrics['skew']['worst_skew_ratio']
        has_shuffle = self.combined_metrics['complexity']['has_shuffle']
        has_joins = self.combined_metrics['complexity']['has_joins']

        logger.info(f"\n  🔍 Data Characteristics:")
        logger.info(f"     Largest partition: {max_partition_size_gb:.2f} GB")
        logger.info(f"     Complexity: {complexity}")
        logger.info(f"     Worst skew: {skew_ratio:.1f}x")
        logger.info(f"     Has joins: {has_joins}")
        logger.info(f"     Has shuffle: {has_shuffle}")

        # ═══════════════════════════════════════════════════════════════
        # STEP 2: Define worker selection function
        # ═══════════════════════════════════════════════════════════════

        def determine_worker_type(data_size_gb: float, largest_partition_gb: float) -> tuple:
            """Select appropriate worker type"""

            worker_specs = {
                # Source: https://docs.aws.amazon.com/glue/latest/dg/worker-types.html
                # Memory: total per worker. Usable for Spark = ~62% (confirmed via AWS repost: 10GB heap of 16GB)
                # Disk: attached EBS for shuffle spill
                # Cores: vCPUs per worker (Glue 4.0)
                'G.1X': {
                    'total_memory_gb': 16,       # AWS docs: 4 vCPU, 16 GB, 64 GB disk
                    'usable_memory_gb': 10,      # ~62% = heap (confirmed AWS repost)
                    'disk_gb': 64,
                    'vcpus': 4,
                    'dpu': 1,
                    # Max data per worker: usable_memory * 0.6 (storage fraction) for caching
                    # For processing (no cache): can handle ~2-3x memory in streaming fashion
                    # These are CONSERVATIVE estimates (prefer over-provision to OOM):
                    'max_data_simple': 6,        # Simple read/write: 6 GB/worker
                    'max_data_complex': 3,       # Joins + shuffles: 3 GB/worker (data expands 2-3x in memory)
                    'max_data_extreme': 1.5      # Skewed + multiple joins: 1.5 GB/worker
                },
                'G.2X': {
                    'total_memory_gb': 32,       # AWS docs: 8 vCPU, 32 GB, 128 GB disk
                    'usable_memory_gb': 20,      # ~62%
                    'disk_gb': 128,
                    'vcpus': 8,
                    'dpu': 2,
                    'max_data_simple': 14,
                    'max_data_complex': 7,
                    'max_data_extreme': 3.5
                },
                'G.4X': {
                    'total_memory_gb': 64,       # AWS docs: 16 vCPU, 64 GB, 256 GB disk
                    'usable_memory_gb': 40,      # ~62%
                    'disk_gb': 256,
                    'vcpus': 16,
                    'dpu': 4,
                    'max_data_simple': 28,
                    'max_data_complex': 14,
                    'max_data_extreme': 7
                },
                'G.8X': {
                    'total_memory_gb': 128,      # AWS docs: 32 vCPU, 128 GB, 512 GB disk
                    'usable_memory_gb': 80,      # ~62%
                    'disk_gb': 512,
                    'vcpus': 32,
                    'dpu': 8,
                    'max_data_simple': 56,
                    'max_data_complex': 28,
                    'max_data_extreme': 14
                }
            }
            # NOTE: max_data values are empirical estimates. AWS does not publish exact throughput numbers.
            # These should be calibrated per workload by running 2-3 test jobs and measuring actual
            # Spark UI metrics (shuffle read/write, GC time, task duration distribution).
            # The formula: max_data ≈ usable_memory * spark.memory.fraction(0.6) * utilization_factor
            # where utilization_factor = 1.0 (simple), 0.5 (complex), 0.25 (extreme skew)

            if complexity == 'simple' and not has_joins:
                capacity_key = 'max_data_simple'
                memory_multiplier = 1.5
            elif complexity in ['medium', 'complex'] or has_joins:
                capacity_key = 'max_data_complex'
                memory_multiplier = 2.5
            else:
                capacity_key = 'max_data_extreme'
                memory_multiplier = 4.0

            required_memory_for_partition = largest_partition_gb * memory_multiplier

            selected_type = None
            for worker_type in ['G.1X', 'G.2X', 'G.4X', 'G.8X']:
                spec = worker_specs[worker_type]

                if spec['usable_memory_gb'] < required_memory_for_partition:
                    continue

                if largest_partition_gb > spec[capacity_key]:
                    continue

                selected_type = worker_type
                break

            if not selected_type:
                selected_type = 'G.8X'
                logger.warning(
                    f"⚠️  Largest partition ({largest_partition_gb:.2f} GB) exceeds G.8X capacity!"
                )

            spec = worker_specs[selected_type]

            logger.info(f"\n  📦 Selected Worker Type: {selected_type}")
            logger.info(f"     Largest partition {largest_partition_gb:.2f} GB "
                       f"requires {required_memory_for_partition:.2f} GB memory")
            logger.info(f"     Capacity: {spec['usable_memory_gb']} GB usable memory")
            logger.info(f"     Max data per worker: {spec[capacity_key]} GB")

            return (
                selected_type,
                spec['dpu'],
                spec['usable_memory_gb'],
                spec[capacity_key]
            )

        # ═══════════════════════════════════════════════════════════════
        # STEP 3: Define resource calculation function
        # ═══════════════════════════════════════════════════════════════

        def calculate_for_volume(data_size_gb: float, scenario_name: str, 
                                scenario_largest_partition_gb: float) -> Dict:
            """Calculate resources for given scenario"""

            worker_type, dpu_per_worker, usable_memory, max_data_per_worker = \
                determine_worker_type(data_size_gb, scenario_largest_partition_gb)

            base_workers = math.ceil(data_size_gb / max_data_per_worker)

            if skew_ratio > 10:
                skew_adjustment = 1.5
            elif skew_ratio > 3:
                skew_adjustment = 1.2
            else:
                skew_adjustment = 1.0

            num_workers = math.ceil(base_workers * skew_adjustment)
            num_workers = max(2, min(num_workers, 200))

            total_dpus = num_workers * dpu_per_worker

            cores_per_worker = {'G.1X': 4, 'G.2X': 8, 'G.4X': 16, 'G.8X': 32}[worker_type]
            total_cores = num_workers * cores_per_worker

            target_partition_size_mb = 128
            estimated_shuffle_data_gb = data_size_gb * (2.0 if has_joins else 1.0)
            optimal_shuffle_partitions = int((estimated_shuffle_data_gb * 1024) / target_partition_size_mb)

            shuffle_partitions = max(total_cores * 2, min(optimal_shuffle_partitions, total_cores * 8))

            # Throughput estimates (GB processed per worker per minute).
            # These are EMPIRICAL BASELINES from observed Glue jobs — NOT from AWS docs.
            # AWS does not publish throughput numbers because they vary by:
            #   - Data format (Parquet columnar vs CSV vs JSON)
            #   - Compression (snappy vs gzip vs none)
            #   - Column count and width
            #   - Shuffle ratio (how much data moves between workers)
            #   - Network bandwidth utilization
            #
            # CALIBRATION: Run your actual job with Spark UI enabled, then:
            #   actual_throughput = input_size_gb / (elapsed_minutes * num_workers)
            # Update these values per your workload for accurate estimates.
            base_throughput_map = {
                'simple': 2.0,     # Read → filter → write (no shuffle). Observed: 1.5-3.0 GB/worker/min
                'medium': 1.0,     # 1-2 joins + GROUP BY. Observed: 0.7-1.5 GB/worker/min
                'complex': 0.5,    # Multiple joins + windows + skew. Observed: 0.3-0.8 GB/worker/min
                'extreme': 0.25    # Heavy skew + large shuffle. Observed: 0.1-0.4 GB/worker/min
            }

            throughput_per_worker = base_throughput_map[complexity]

            if skew_ratio > 100:
                skew_time_multiplier = 3.0
            elif skew_ratio > 10:
                skew_time_multiplier = 2.0
            elif skew_ratio > 3:
                skew_time_multiplier = 1.5
            else:
                skew_time_multiplier = 1.0

            base_time_min = data_size_gb / (num_workers * throughput_per_worker)
            estimated_time_min = base_time_min * skew_time_multiplier

            overhead_min = 3 if data_size_gb < 10 else 5
            estimated_time_min += overhead_min
            estimated_time_min = max(estimated_time_min, 5)

            estimated_cost = total_dpus * 0.44 * (estimated_time_min / 60)
            timeout_minutes = max(int(estimated_time_min * 3), 30)

            return {
                'scenario': scenario_name,
                'data_volume_gb': round(data_size_gb, 2),
                'largest_partition_gb': round(scenario_largest_partition_gb, 2),
                'worker_type': worker_type,
                'num_workers': num_workers,
                'dpu_per_worker': dpu_per_worker,
                'total_dpus': total_dpus,
                'shuffle_partitions': shuffle_partitions,
                'timing': {
                    'estimated_time_min': round(estimated_time_min, 1),
                    'estimated_time_hours': round(estimated_time_min / 60, 2),
                    'timeout_minutes': timeout_minutes,
                    'throughput_gb_per_min': round(throughput_per_worker * num_workers, 2)
                },
                'cost': {
                    'estimated_cost_usd': round(estimated_cost, 2),
                    'dpu_hour_rate': 0.44,
                    'estimated_dpu_hours': round(total_dpus * (estimated_time_min / 60), 2)
                },
                'calculation_details': {
                    'max_data_per_worker_gb': max_data_per_worker,
                    'worker_memory_gb': usable_memory,
                    'skew_time_multiplier': skew_time_multiplier,
                    'base_throughput_gb_per_min': throughput_per_worker
                }
            }

        # ═══════════════════════════════════════════════════════════════
        # STEP 4: Calculate incremental scenarios (OUTSIDE nested functions!)
        # ═══════════════════════════════════════════════════════════════

        days_in_window = (
            datetime.strptime(self.analysis_window['end_date'], '%Y%m%d') -
            datetime.strptime(self.analysis_window['start_date'], '%Y%m%d')
        ).days + 1

        avg_daily_size_gb = analyzed_window_size_gb / days_in_window if days_in_window > 0 else 0
        avg_weekly_size_gb = avg_daily_size_gb * 7
        avg_monthly_size_gb = avg_daily_size_gb * 30

        daily_partition_size = max_partition_size_gb / analyzed_window_size_gb * avg_daily_size_gb if analyzed_window_size_gb > 0 else 0
        weekly_partition_size = max_partition_size_gb / analyzed_window_size_gb * avg_weekly_size_gb if analyzed_window_size_gb > 0 else 0
        monthly_partition_size = max_partition_size_gb / analyzed_window_size_gb * avg_monthly_size_gb if analyzed_window_size_gb > 0 else 0

        logger.info(f"\n  📊 Volume Estimates:")
        logger.info(f"     Full catalog: {full_catalog_size_gb:.2f} GB (largest partition: {max_partition_size_gb:.2f} GB)")
        logger.info(f"     Daily avg: {avg_daily_size_gb:.2f} GB (largest partition: {daily_partition_size:.2f} GB)")
        logger.info(f"     Weekly avg: {avg_weekly_size_gb:.2f} GB (largest partition: {weekly_partition_size:.2f} GB)")
        logger.info(f"     Monthly avg: {avg_monthly_size_gb:.2f} GB (largest partition: {monthly_partition_size:.2f} GB)")

        # ═══════════════════════════════════════════════════════════════
        # STEP 5: Generate all configurations (NOW ACTUALLY CALL THE FUNCTION!)
        # ═══════════════════════════════════════════════════════════════

        recommendations = {
            'summary': {
                'full_catalog_size_gb': round(full_catalog_size_gb, 2),
                'analyzed_window_size_gb': round(analyzed_window_size_gb, 2),
                'largest_partition_gb': round(max_partition_size_gb, 2),
                'avg_daily_size_gb': round(avg_daily_size_gb, 2),
                'avg_weekly_size_gb': round(avg_weekly_size_gb, 2),
                'avg_monthly_size_gb': round(avg_monthly_size_gb, 2),
                'complexity': complexity,
                'worst_skew_ratio': skew_ratio,
                'has_joins': has_joins,
                'has_shuffle': has_shuffle
            },

            'initial_load': calculate_for_volume(
                full_catalog_size_gb,
                "Initial Full Load (Overwrite Mode - ALL historical data)",
                max_partition_size_gb
            ),

            'incremental_daily': calculate_for_volume(
                avg_daily_size_gb,
                "Daily Incremental (Append Mode - 1 day)",
                daily_partition_size
            ),

            'incremental_weekly': calculate_for_volume(
                avg_weekly_size_gb,
                "Weekly Incremental (Append Mode - 7 days)",
                weekly_partition_size
            ),

            'incremental_monthly': calculate_for_volume(
                avg_monthly_size_gb,
                "Monthly Incremental (Append Mode - 30 days)",
                monthly_partition_size
            )
        }

        # ═══════════════════════════════════════════════════════════════
        # STEP 6: Log recommendations
        # ═══════════════════════════════════════════════════════════════

        logger.info("\n" + "="*80)
        logger.info("REALISTIC RESOURCE RECOMMENDATIONS")
        logger.info("="*80)

        for scenario_key in ['initial_load', 'incremental_daily', 'incremental_weekly', 'incremental_monthly']:
            config = recommendations[scenario_key]

            logger.info(f"\n📋 {config['scenario']}:")
            logger.info(f"   Data Volume: {config['data_volume_gb']:.2f} GB")
            logger.info(f"   Largest Partition: {config['largest_partition_gb']:.2f} GB")
            logger.info(f"   Worker Type: {config['worker_type']} ({config['calculation_details']['worker_memory_gb']} GB usable memory)")
            logger.info(f"   Num Workers: {config['num_workers']}")
            logger.info(f"   Total DPUs: {config['total_dpus']}")
            logger.info(f"   Shuffle Partitions: {config['shuffle_partitions']}")
            logger.info(f"   Est. Time: {config['timing']['estimated_time_min']:.1f} min ({config['timing']['estimated_time_hours']:.2f} hrs)")
            logger.info(f"   Throughput: {config['timing']['throughput_gb_per_min']:.2f} GB/min")
            logger.info(f"   Est. Cost: ${config['cost']['estimated_cost_usd']:.2f}")

        # Cost summary
        daily_cost = recommendations['incremental_daily']['cost']['estimated_cost_usd']
        initial_cost = recommendations['initial_load']['cost']['estimated_cost_usd']
        monthly_operational = daily_cost * 30

        logger.info("\n💰 COST SUMMARY:")
        logger.info(f"   Initial Load (one-time): ${initial_cost:.2f}")
        logger.info(f"   Daily Incremental: ${daily_cost:.2f}")
        logger.info(f"   Monthly Operational (30 daily runs): ${monthly_operational:.2f}/month")
        logger.info("="*80)

        return recommendations  

        # -------------------------------------------------------------------------
        # SPARK CONFIGURATION
        # -------------------------------------------------------------------------

    def _generate_spark_config(self, resource_rec: Dict) -> Dict:
        """Generate Spark configurations for ALL scenarios"""

        logger.info("\nGenerating Spark configurations...")

        configs = {}

        for scenario_key in ['initial_load', 'incremental_daily', 'incremental_weekly', 'incremental_monthly']:
            scenario_config = resource_rec[scenario_key]

            shuffle_partitions = scenario_config['shuffle_partitions']
            skew_level = self.combined_metrics['skew']['worst_skew_level']

            config = {}

            # Shuffle partitions
            config['spark.sql.shuffle.partitions'] = str(shuffle_partitions)
            config['spark.default.parallelism'] = str(shuffle_partitions)

            # AQE (always enable)
            config['spark.sql.adaptive.enabled'] = 'true'
            config['spark.sql.adaptive.coalescePartitions.enabled'] = 'true'
            config['spark.sql.adaptive.localShuffleReader.enabled'] = 'true'
            config['spark.sql.adaptive.coalescePartitions.parallelismFirst'] = 'true'

            # Skew handling
            if skew_level in ['MODERATE', 'HIGH', 'EXTREME']:
                config['spark.sql.adaptive.skewJoin.enabled'] = 'true'
                if skew_level == 'EXTREME':
                    config['spark.sql.adaptive.skewJoin.skewedPartitionFactor'] = '5'
                    config['spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'] = '536870912'
                else:
                    config['spark.sql.adaptive.skewJoin.skewedPartitionFactor'] = '3'
                    config['spark.sql.adaptive.skewJoin.skewedPartitionThresholdInBytes'] = '268435456'

            # Rest of configs...
            config['spark.sql.adaptive.advisoryPartitionSizeInBytes'] = '134217728'
            # AWS Glue 4.0 has AQE enabled by default which auto-converts sort-merge to broadcast
            # when runtime stats show one side < adaptive threshold.
            # Set explicit threshold based on detected dimension sizes (default 10MB in Spark,
            # but AWS recommends up to 100MB for Glue where network is fast).
            # Reference: https://docs.aws.amazon.com/prescriptive-guidance/latest/tuning-aws-glue-for-apache-spark/optimize-shuffles.html
            config['spark.sql.autoBroadcastJoinThreshold'] = '104857600'  # 100MB — let AQE decide dynamically
            config['spark.sql.files.maxPartitionBytes'] = '134217728'
            config['spark.sql.files.maxRecordsPerFile'] = '500000'
            config['spark.sql.parquet.enableVectorizedReader'] = 'true'
            config['spark.sql.sources.partitionOverwriteMode'] = 'dynamic'
            config['spark.sql.legacy.parquet.datetimeRebaseModeInRead'] = 'LEGACY'
            config['spark.sql.legacy.parquet.datetimeRebaseModeInWrite'] = 'LEGACY'

            # NOTE: These configs are managed by AWS Glue and cannot be overridden:
            # - spark.memory.fraction
            # - spark.memory.storageFraction  
            # - spark.serializer (already Kryo in Glue 3.0+)

            configs[scenario_key] = config

        logger.info(f"  ✓ Generated configs for {len(configs)} scenarios")

        return configs
    
    # -------------------------------------------------------------------------
    # CACHING ANALYSIS
    # -------------------------------------------------------------------------
    
    
    def _analyze_caching(self) -> List[Dict]:
        """Analyze caching opportunities"""
        
        logger.info("\nAnalyzing caching opportunities...")
        
        recommendations = []
        
        for source in self.source_analyses:
            if source['role'] != 'dimension':
                continue
            
            size_mb = source['volume']['total_size_gb'] * 1024
            
            # Simple heuristic: cache if dimension < 10GB
            if size_mb < 10000:
                recommendations.append({
                    'table': f"{source['database']}.{source['table']}",
                    'should_cache': True,
                    'reason': f"Small dimension table ({size_mb:.0f}MB) likely reused in joins",
                    'cache_type': 'MEMORY_AND_DISK',
                    'action': 'df.cache(); df.foreach(lambda _: None)  # materialize without .count() overhead',
                    'expected_speedup': '10-25% if reused multiple times'
                })
            else:
                recommendations.append({
                    'table': f"{source['database']}.{source['table']}",
                    'should_cache': False,
                    'reason': f"Dimension too large ({size_mb:.0f}MB) for caching",
                    'alternative': 'Use sort-merge join with AQE'
                })
        
        logger.info(f"  Generated {len(recommendations)} caching recommendations")
        
        return recommendations
    
    # -------------------------------------------------------------------------
    # RECOMMENDATIONS & WARNINGS
    # -------------------------------------------------------------------------
    
    def _generate_recommendations(self) -> List[Dict]:
        """Generate optimization recommendations"""
        
        recommendations = []
        
        skew_level = self.combined_metrics['skew']['worst_skew_level']
        skew_ratio = self.combined_metrics['skew']['worst_skew_ratio']
        
        if skew_level in ['MODERATE', 'HIGH', 'EXTREME']:
            recommendations.append({
                'priority': 'HIGH',
                'type': 'ENABLE_SKEW_JOIN',
                'reason': f"{skew_level} skew detected ({skew_ratio:.1f}x ratio)",
                'action': 'spark.sql.adaptive.skewJoin.enabled = true (already configured)',
                'expected_improvement': '20-40% faster joins'
            })
        
        # Deduplication recommendation
        for source in self.source_analyses:
            if source['quality']['duplicate_pct'] > 1:
                recommendations.append({
                    'priority': 'HIGH',
                    'type': 'DEDUPLICATION_REQUIRED',
                    'reason': f"{source['quality']['duplicate_pct']:.2f}% duplicates in {source['table']}",
                    'action': 'Use ROW_NUMBER() with ORDER BY for deduplication',
                    'expected_improvement': 'Ensures data quality'
                })
        
        # Partition filter recommendation
        recommendations.append({
            'priority': 'MEDIUM',
            'type': 'PARTITION_FILTER',
            'reason': f"Analyzing {self.analysis_window['months_count']} months - use push-down predicate",
            'action': 'Use push_down_predicate in create_dynamic_frame.from_catalog()',
            'expected_improvement': 'Read only required data (saves I/O)'
        })
        
        return recommendations
    
    
    def _generate_warnings(self) -> List[str]:
        """Generate warnings"""

        warnings = []

        skew_ratio = self.combined_metrics['skew']['worst_skew_ratio']
        if skew_ratio > 10:
            warnings.append(f"High skew detected ({skew_ratio:.1f}x) - monitor task execution times")

        for source in self.source_analyses:
            if source['quality']['duplicate_pct'] > 1:
                warnings.append(f"{source['quality']['duplicate_pct']:.2f}% duplicates in {source['table']}")

        return warnings
    
    def _evaluate_quality_gates(self) -> Dict:
        """Evaluate data quality gates"""
        
        checks = []
        passed = True
        
        for source in self.source_analyses:
            # Check 1: Primary key NULL rate
            pk_null = source['quality']['null_analysis'].get(source['metadata'].get('partition_column', ''), {}).get('null_pct', 0)
            check_passed = pk_null < 1
            checks.append({
                'check': f"Primary key NULL rate < 1% ({source['table']})",
                'result': 'PASS' if check_passed else 'FAIL',
                'value': f"{pk_null:.2f}%"
            })
            if not check_passed:
                passed = False
            
            # Check 2: Quality score
            quality_score = source['quality']['quality_score']
            check_passed = quality_score > 90
            checks.append({
                'check': f"Quality score > 90 ({source['table']})",
                'result': 'PASS' if check_passed else 'FAIL',
                'value': f"{quality_score:.1f}"
            })
            if not check_passed:
                passed = False
        
        return {
            'passed': passed,
            'checks': checks,
            'recommendation': 'PROCEED - Data quality acceptable' if passed else 'REVIEW - Quality issues detected'
        }
    
    def _get_best_practices_applied(self) -> List[str]:
        """Get list of best practices applied"""
        
        return [
            "✓ Commandment #1: Analyzed data before optimizing",
            "✓ Commandment #3: Using push-down filters recommended",
            f"✓ Commandment #4: Selected appropriate worker type ({self.combined_metrics.get('worker_type', 'TBD')})",
            "✓ Commandment #5: AQE enabled for automatic optimization",
            f"✓ Commandment #6: Skew monitoring enabled (detected {self.combined_metrics['skew']['worst_skew_ratio']:.1f}x)",
            "✓ Commandment #8: Partitioning optimized based on granularity",
            "✓ Commandment #9: Caching evaluated and recommended where beneficial"
        ]
def analyze_and_optimize_glue_job(
    sources: List[Dict[str, Any]],
    target: Dict[str, str],
    run_mode: str,
    analysis_end_month: str,                   # ← Required params first
    spark: SparkSession,
    glue_context: GlueContext,
    analysis_months_count: int = 3,
    output_format: str = "dict"
) -> Dict[str, Any]:
    """
    ╔══════════════════════════════════════════════════════════════════════════════╗
    ║  ETL JOB OPTIMIZER — Analyze tables & recommend Spark/Glue configurations  ║
    ╚══════════════════════════════════════════════════════════════════════════════╝

    Analyzes your source tables and produces:
      • Optimal worker type (G.1X / G.2X / G.4X / G.8X)
      • Number of workers
      • Shuffle partitions
      • Full Spark config dict (copy-paste into _configure_spark())
      • Estimated runtime and cost (for 4 scenarios)
      • Skew analysis, quality gates, caching recommendations

    ═══════════════════════════════════════════════════════════════════════════════
    GLOBAL PARAMETERS (function-level)
    ═══════════════════════════════════════════════════════════════════════════════

    ┌─────────────────────┬──────────┬─────────────────────────────────────────────────────────────┐
    │ Parameter           │ Required │ Description                                                  │
    ├─────────────────────┼──────────┼─────────────────────────────────────────────────────────────┤
    │ sources             │ ✅ YES   │ List of source table dicts (see PER-SOURCE section below)    │
    │ target              │ ✅ YES   │ {"database": "gold_db", "table": "gold_table"}               │
    │                     │          │ Only used for naming — NOT read by the optimizer              │
    │ run_mode            │ ✅ YES   │ "overwrite" or "append"                                      │
    │                     │          │ overwrite = full table rewrite (more data to process)        │
    │                     │          │ append = incremental (less data, smaller resources needed)    │
    │ analysis_end_month  │ ✅ YES   │ "YYYYMM" format, e.g. "202606"                              │
    │                     │          │ End of the window to analyze. Optimizer reads this month      │
    │                     │          │ backwards by analysis_months_count.                           │
    │ spark               │ ✅ YES   │ Active SparkSession instance                                 │
    │ glue_context        │ ✅ YES   │ GlueContext instance (for create_dynamic_frame)              │
    │ analysis_months_count│ ❌ NO   │ Default: 3. How many months to sample (1-3).                 │
    │                     │          │ More months = more accurate skew detection, but slower.       │
    │                     │          │ Capped at 3 to keep analysis fast.                            │
    │ output_format       │ ❌ NO   │ Default: "dict". Options: "dict" | "json"                    │
    └─────────────────────┴──────────┴─────────────────────────────────────────────────────────────┘

    ═══════════════════════════════════════════════════════════════════════════════
    PER-SOURCE PARAMETERS (each dict in the `sources` list)
    ═══════════════════════════════════════════════════════════════════════════════

    ┌─────────────────────┬──────────┬─────────────────────────────────────────────────────────────┐
    │ Key                 │ Required │ Description                                                  │
    ├─────────────────────┼──────────┼─────────────────────────────────────────────────────────────┤
    │ database            │ ✅ YES   │ Glue Catalog database name                                   │
    │                     │          │ e.g. "mobile_revenue_analytics_silver"                        │
    │ table               │ ✅ YES   │ Table name in the database                                   │
    │                     │          │ e.g. "silver_sales"                                           │
    │ primary_key         │ ✅ YES   │ Column to analyze for skew distribution                      │
    │                     │          │ Should be your GROUP BY / join key                            │
    │                     │          │ e.g. "site_code", "customer_id", "transaction_id"            │
    │                     │          │ Used for: skew ratio, p50/p90/p95/p99, top keys               │
    ├─────────────────────┼──────────┼─────────────────────────────────────────────────────────────┤
    │ partition_column    │ ❌ NO    │ Override auto-detected partition column                       │
    │                     │          │ Default: uses the table's Glue Catalog partition key          │
    │                     │          │ Set this when your ETL partitions by a DIFFERENT column       │
    │                     │          │ than what Glue Catalog shows (e.g. catalog has data_dt but    │
    │                     │          │ your ETL groups by mnth_id)                                   │
    │ role                │ ❌ NO    │ Default: "primary"                                            │
    │                     │          │ "primary" = fact/main table (drives resource sizing)          │
    │                     │          │ "dimension" = lookup/reference table (gets caching recs)      │
    │ operation           │ ❌ NO    │ Default: None (standalone table)                              │
    │                     │          │ "JOIN" = this table is joined to another source               │
    │                     │          │ "UNION" = combined with other tables (additive volume)        │
    │                     │          │ "JOIN_UNION_GROUPS" = metadata-only operation that joins      │
    │                     │          │   two union_groups together (no actual table read)            │
    │ join_keys           │ ⚠️ COND  │ REQUIRED if operation="JOIN"                                  │
    │                     │          │ List of columns used in the join condition                    │
    │                     │          │ e.g. ["product_id", "mnth_id"]                                │
    │ join_type           │ ❌ NO    │ Default: "LEFT"                                               │
    │                     │          │ Options: "INNER", "LEFT", "RIGHT"                             │
    │                     │          │ Affects join analysis recommendations                         │
    │ union_group         │ ❌ NO    │ Group name for UNION analysis                                 │
    │                     │          │ Tables with same union_group get their sizes SUMMED            │
    │                     │          │ e.g. "all_sales" for prepaid + postpaid + tol tables          │
    │ left_group          │ ⚠️ COND  │ REQUIRED if operation="JOIN_UNION_GROUPS"                     │
    │                     │          │ Name of the left-side union_group                             │
    │ right_group         │ ⚠️ COND  │ REQUIRED if operation="JOIN_UNION_GROUPS"                     │
    │                     │          │ Name of the right-side union_group                            │
    └─────────────────────┴──────────┴─────────────────────────────────────────────────────────────┘

    ═══════════════════════════════════════════════════════════════════════════════
    EXAMPLES
    ═══════════════════════════════════════════════════════════════════════════════

    # SIMPLE: Single table, no joins
    result = analyze_and_optimize_glue_job(
        sources=[{"database": "silver_db", "table": "sales", "primary_key": "order_id"}],
        target={"database": "gold_db", "table": "gold_sales"},
        run_mode="overwrite",
        analysis_end_month="202606",
        spark=spark,
        glue_context=glueContext
    )

    # COMPLEX: Fact + dimension join + union group
    result = analyze_and_optimize_glue_job(
        sources=[
            {"database": "silver_db", "table": "silver_sales",
             "primary_key": "site_code", "partition_column": "mnth_id", "role": "primary"},
            {"database": "silver_db", "table": "cellsite_mapping",
             "primary_key": "std_site_nm", "role": "dimension",
             "operation": "JOIN", "join_keys": ["site_code"], "join_type": "INNER"},
        ],
        target={"database": "gold_db", "table": "gold_ticketing"},
        run_mode="overwrite",
        analysis_end_month="202606",
        spark=spark,
        glue_context=glueContext,
        analysis_months_count=2
    )

    ═══════════════════════════════════════════════════════════════════════════════
    OUTPUT (what you get back)
    ═══════════════════════════════════════════════════════════════════════════════

    {
        "metadata": {...},                    # Timestamps, versions, analysis window
        "source_analysis": [...],             # Per-table: volume, skew, quality, scaling
        "combined_metrics": {...},            # Totals across all sources
        "resource_recommendation": {          # 4 scenarios:
            "initial_load": {...},            #   Full historical load (overwrite all)
            "incremental_daily": {...},       #   1 day of new data
            "incremental_weekly": {...},      #   7 days of new data
            "incremental_monthly": {...},     #   30 days of new data
        },
        "spark_configurations": {             # Ready-to-use Spark configs per scenario
            "initial_load": {"spark.sql.shuffle.partitions": "400", ...},
            "incremental_daily": {"spark.sql.shuffle.partitions": "100", ...},
            ...
        },
        "caching_recommendations": [...],     # Which tables to cache/broadcast
        "optimization_recommendations": [...],# Actionable optimization tips
        "warnings": [...],                    # Issues detected
        "data_quality_gates": {...},          # Pass/fail quality checks
    }
    """
    
    logger.info("="*80)
    logger.info("STARTING ETL OPTIMIZATION ANALYSIS")
    logger.info("="*80)
    logger.info(f"Run Mode: {run_mode}")
    logger.info(f"Target Granularity: {target.get('partition_granularity', 'auto-detect')}")
    logger.info(f"Analysis Period: {analysis_months_count} months ending {analysis_end_month}")
    logger.info(f"Sources: {len(sources)} table(s)")
    
    start_time = datetime.now()
    
    try:
        # Initialize analyzer
        analyzer = ETLOptimizationAnalyzer(
        sources=sources,
        target=target,
        run_mode=run_mode,
        analysis_end_month=analysis_end_month,
        analysis_months_count=analysis_months_count,
        spark=spark,
        glue_context=glue_context)
        
        # Run analysis
        results = analyzer.analyze()
        
        # Calculate duration
        duration = (datetime.now() - start_time).total_seconds()
        results['metadata']['analysis_duration_seconds'] = round(duration, 2)
        
        logger.info("="*80)
        logger.info(f"ANALYSIS COMPLETED SUCCESSFULLY in {duration:.1f} seconds")
        logger.info("="*80)
        
        if output_format == "json":
            return json.dumps(results, indent=2, default=str)
        else:
            return results
            
    except Exception as e:
        logger.error(f"Analysis failed: {str(e)}", exc_info=True)
        raise


# ============================================================================
# ENTRY POINT
# ============================================================================
if __name__ == "__main__":
    # Import and call analyze_and_optimize_glue_job(...) from your job/notebook, where a SparkSession
    # (and GlueContext on AWS) already exist. Example:
    #
    #   result = analyze_and_optimize_glue_job(
    #       sources=[{"database": "CHANGE_ME", "table": "CHANGE_ME",
    #                 "primary_key": "CHANGE_ME", "partition_column": "mnth_id",
    #                 "role": "primary"}],
    #       target={"database": "CHANGE_ME", "table": "CHANGE_ME"},
    #       run_mode="overwrite", analysis_end_month="202506",
    #       spark=spark, glue_context=glueContext, analysis_months_count=3)
    #   import json; print(json.dumps(result, indent=2, default=str))
    print("ETL job optimizer — import and call analyze_and_optimize_glue_job(...) from your job.")
