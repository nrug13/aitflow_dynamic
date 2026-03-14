from datetime import datetime
from airflow.decorators import dag, task
from airflow.models import Variable


@dag(
    dag_id="delta_vacuum_dynamic",
    start_date=datetime(2025, 1, 1),
    schedule="@weekly",
    catchup=False,
    tags=["delta", "vacuum", "maintenance"],
)
def delta_vacuum_dynamic():

    @task
    def get_vacuum_configs() -> list[dict]:
        """Airflow Variable-dan delta cedvel konfiqurasiyalarini oxuyur."""
        raw = Variable.get("delta_vacuum_config", deserialize_json=True)
        return raw

    @task
    def run_vacuum(config: dict):
        import subprocess

        delta_path = config["delta_path"]
        retention = config["retention_hours"]
        table_name = config["table_name"]

        cmd = [
            "spark-submit",
            "--master", "local[*]",
            "--name", f"vacuum_{table_name}",
            "/opt/airflow/dags/scripts/vacuum_delta.py",
            "--delta_path", delta_path,
            "--retention_hours", str(retention),
        ]

        print(f"Running: {' '.join(cmd)}")
        result = subprocess.run(cmd, capture_output=True, text=True)

        print(result.stdout)
        if result.returncode != 0:
            print(result.stderr)
            raise Exception(
                f"Vacuum failed for {table_name}: {result.stderr}"
            )

    configs = get_vacuum_configs()
    run_vacuum.expand(config=configs)


delta_vacuum_dynamic()
