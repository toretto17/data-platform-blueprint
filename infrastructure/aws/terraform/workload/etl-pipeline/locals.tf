/*
 * Glue Jobs Configuration
 * Add your jobs to gluejobs_list. Each needs at minimum: name.
 * Optional: worker_type, number_of_workers, timeout, extra_jars, additional_arguments.
 */
locals {
  gluejobs_list = [
    # --- Silver Jobs ---
    {
      name              = "glue_${var.project}_${var.feature}_silver_CHANGE_ME"
      worker_type       = "G.1X"
      number_of_workers = 2
      timeout           = 120
    },

    # --- Gold Jobs ---
    {
      name              = "glue_${var.project}_${var.feature}_gold_CHANGE_ME"
      worker_type       = "G.2X"
      number_of_workers = 5
      timeout           = 180
    },

    # --- Consumption Jobs ---
    {
      name              = "glue_${var.project}_${var.feature}_consumpt_CHANGE_ME"
      worker_type       = "G.2X"
      number_of_workers = 10
      timeout           = 120
    },

    # --- Feature Store Jobs (need extra JAR) ---
    {
      name              = "glue_${var.project}_${var.feature}_CHANGE_ME_feature_store"
      worker_type       = "G.2X"
      number_of_workers = 5
      timeout           = 60
      extra_jars        = "spark-connector-jars/sagemaker-feature-store-spark-sdk-3.5.jar"
    },
  ]

  # --- Step Functions ---
  sfn_list = [
    {
      name        = "${var.project}-CHANGE_ME-master-pipeline"
      config_file = "CHANGE_ME-master-pipeline_sfn_config.json"
    },
  ]

  # --- EventBridge Schedules ---
  schedules_list = [
    {
      name           = "${var.project}-CHANGE_ME-pipeline"
      cron           = "cron(0 18 * * ? *)"  # Daily 01:00 AM local (adjust UTC offset)
      sf_name        = "${var.project}-CHANGE_ME-master-pipeline"
      enabled        = true
      input_template = "#{aws:CurrentTime}"  # Or static: "2026-01-01T00:00:00Z"
    },
  ]
}
