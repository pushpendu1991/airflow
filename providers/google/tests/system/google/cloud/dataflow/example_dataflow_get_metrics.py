# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

"""
Example Airflow DAG for Google Cloud Dataflow Get Metrics Operator.

This DAG demonstrates how to use DataflowJobMetricsOperator to:
1. Collect metrics from a running Dataflow job
2. Route metrics to Pub/Sub for real-time streaming
3. Stream metrics into BigQuery for historical analysis
4. Use both destinations simultaneously (fan-out)
5. Consume metrics summary from XCom in downstream tasks
"""

import os
from datetime import datetime

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowJobMetricsOperator

from system.google import DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID

ENV_ID = os.environ.get("SYSTEM_TESTS_ENV_ID", "default")
PROJECT_ID = os.environ.get("SYSTEM_TESTS_GCP_PROJECT") or DEFAULT_GCP_SYSTEM_TEST_PROJECT_ID
DAG_ID = "dataflow_get_metrics"
LOCATION = "us-central1"
DATAFLOW_JOB_NAME = f"dataflow-example-job-{ENV_ID}"
PUBSUB_TOPIC = f"projects/{PROJECT_ID}/topics/dataflow-metrics-{ENV_ID}"
BQ_DATASET = "dataflow_metrics"
BQ_TABLE = "job_metrics"
BQ_PROJECT = PROJECT_ID
BQ_DATASET_LOCATION = LOCATION

def print_metrics_summary(**context):
    """
    Print the metrics summary pushed to XCom by DataflowJobMetricsOperator.
    Demonstrates how to consume metrics data in downstream tasks.
    """
    task_instance = context["task_instance"]
    
    metrics_summary = task_instance.xcom_pull(task_ids="collect_metrics_bigquery", key="metrics_summary")
    
    if metrics_summary:
        print("=" * 70)
        print("DATAFLOW JOB METRICS SUMMARY")
        print("=" * 70)
        print(f"Job ID:                 {metrics_summary.get('job_id')}")
        print(f"Metric Count:           {metrics_summary.get('metric_count')}")
        print(f"BigQuery Destination:   {metrics_summary.get('bq_destination')}")
        print(f"BigQuery Rows Written:  {metrics_summary.get('bq_rows_written')}")
        print(f"Pub/Sub Topic:          {metrics_summary.get('pubsub_topic')}")
        print(f"Pub/Sub Published:      {metrics_summary.get('pubsub_metrics_published')}")
        print("=" * 70)
    else:
        print("No metrics summary found in XCom")

with DAG(
    DAG_ID,
    schedule="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["example", "dataflow", "metrics"],
) as dag:

    start_task = EmptyOperator(task_id="start_task")

    # [START howto_operator_dataflow_get_metrics_bigquery]
    collect_metrics_bigquery = DataflowJobMetricsOperator(
        task_id="collect_metrics_bigquery",
        job_id="{{ dag_run.conf.get('dataflow_job_id', 'test-job-id') }}",
        project_id=PROJECT_ID,
        location=LOCATION,
        bq_dataset=BQ_DATASET,
        bq_table=BQ_TABLE,
        bq_dataset_location=BQ_DATASET_LOCATION,
        bq_project=BQ_PROJECT,
        deferrable=False,
        gcp_conn_id="google_cloud_default",
    )
    # [END howto_operator_dataflow_get_metrics_bigquery]

    # [START howto_operator_dataflow_get_metrics_pubsub_deferrable]
    collect_metrics_pubsub = DataflowJobMetricsOperator(
        task_id="collect_metrics_pubsub",
        job_id="{{ dag_run.conf['dataflow_job_id'] or 'test-job-id' }}",
        project_id=PROJECT_ID,
        location=LOCATION,
        pubsub_topic=PUBSUB_TOPIC,
        deferrable=True,
        poll_sleep=10,
        gcp_conn_id="google_cloud_default",
    )
    # [END howto_operator_dataflow_get_metrics_pubsub_deferrable]

    # [START howto_operator_dataflow_get_metrics_multi_destination]
    collect_metrics_multi_destination = DataflowJobMetricsOperator(
        task_id="collect_metrics_fanout",
        job_id="{{ dag_run.conf['dataflow_job_id'] or 'test-job-id' }}",
        project_id=PROJECT_ID,
        location=LOCATION,
        pubsub_topic=PUBSUB_TOPIC,
        bq_dataset=BQ_DATASET,
        bq_table=BQ_TABLE,
        bq_dataset_location=BQ_DATASET_LOCATION,
        bq_project=BQ_PROJECT,
        deferrable=True,
        poll_sleep=10,
        gcp_conn_id="google_cloud_default",
    )
    # [END howto_operator_dataflow_get_metrics_multi_destination]

    # [START howto_operator_dataflow_get_metrics_xcom_processing]
    process_metrics_summary = PythonOperator(
        task_id="process_metrics_summary",
        python_callable=print_metrics_summary,
    )
    # [END howto_operator_dataflow_get_metrics_xcom_processing]

    end_task = EmptyOperator(task_id="end_task")

    start_task >> [
        collect_metrics_bigquery,
        collect_metrics_pubsub,
        collect_metrics_multi_destination,
    ] >> process_metrics_summary >> end_task

from tests_common.test_utils.watcher import watcher  # noqa: E402

# This test needs watcher to properly mark success/failure
# when "teardown" task with trigger rule is part of the DAG
list(dag.tasks) >> watcher()

from tests_common.test_utils.system_tests import get_test_run  # noqa: E402

# Needed to run the example DAG with pytest (see: tests/system/README.md#run_via_pytest)
test_run = get_test_run(dag)
