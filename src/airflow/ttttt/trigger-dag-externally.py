# File: trigger_dag.py
# This file contains different methods to trigger DAGs externally

import requests
import json
from datetime import datetime
import logging
from typing import Dict, Any, Optional
import os

class AirflowAPITrigger:
    def __init__(self, host: str, username: str, password: str):
        """
        Initialize Airflow API trigger with authentication details

        Args:
            host: Airflow webserver host URL
            username: Airflow username
            password: Airflow password
        """
        self.host = host.rstrip('/')
        self.username = username
        self.password = password
        self.token = None
        self.logger = logging.getLogger(__name__)

    def _get_auth_token(self) -> str:
        """Get authentication token from Airflow API"""
        auth_url = f"{self.host}/api/v1/security/login"
        auth_payload = {
            "username": self.username,
            "password": self.password
        }

        try:
            response = requests.post(auth_url, json=auth_payload)
            response.raise_for_status()
            self.token = response.json()["access_token"]
            return self.token
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get auth token: {str(e)}")
            raise

    def trigger_dag(self,
                    dag_id: str,
                    conf: Optional[Dict[str, Any]] = None,
                    execution_date: Optional[str] = None,
                    run_id: Optional[str] = None) -> Dict[str, Any]:
        """
        Trigger a DAG run via Airflow REST API

        Args:
            dag_id: ID of the DAG to trigger
            conf: Optional configuration parameters
            execution_date: Optional specific execution date
            run_id: Optional specific run ID

        Returns:
            Dict containing API response
        """
        if not self.token:
            self._get_auth_token()

        endpoint = f"{self.host}/api/v1/dags/{dag_id}/dagRuns"
        headers = {
            "Authorization": f"Bearer {self.token}",
            "Content-Type": "application/json"
        }

        payload = {
            "conf": conf or {},
            "execution_date": execution_date or datetime.now().isoformat(),
        }

        if run_id:
            payload["run_id"] = run_id

        try:
            response = requests.post(endpoint, headers=headers, json=payload)
            response.raise_for_status()
            result = response.json()
            self.logger.info(f"Successfully triggered DAG {dag_id}. Run ID: {result.get('dag_run_id')}")
            return result
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to trigger DAG {dag_id}: {str(e)}")
            raise

    def get_dag_status(self, dag_id: str, run_id: str) -> Dict[str, Any]:
        """Get status of a specific DAG run"""
        if not self.token:
            self._get_auth_token()

        endpoint = f"{self.host}/api/v1/dags/{dag_id}/dagRuns/{run_id}"
        headers = {"Authorization": f"Bearer {self.token}"}

        try:
            response = requests.get(endpoint, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to get DAG status: {str(e)}")
            raise

# Example usage functions
def trigger_example():
    # Initialize trigger with your Airflow details
    trigger = AirflowAPITrigger(
        host="http://your-airflow-host:8080",
        username="your_username",
        password="your_password"
    )

    # Example configuration
    conf = {
        "data_date": "2024-01-01",
        "parameters": {
            "source": "external_system",
            "process_type": "full_load",
            "email_notification": "user@example.com"
        }
    }

    try:
        # Trigger the DAG
        result = trigger.trigger_dag(
            dag_id="example_external_trigger_dag",
            conf=conf,
            run_id=f"manual_trigger_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        )

        # Get DAG run status
        run_id = result["dag_run_id"]
        status = trigger.get_dag_status("example_external_trigger_dag", run_id)

        print(f"DAG triggered successfully. Run ID: {run_id}")
        print(f"Current status: {status['state']}")

    except Exception as e:
        print(f"Error triggering DAG: {str(e)}")

if __name__ == "__main__":
    trigger_example()