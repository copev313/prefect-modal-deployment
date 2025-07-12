import modal


PREFECT_VERSION = "3.4.7"
PYTHON_VERSION = "3.12"

API_HOST = "0.0.0.0"
API_PORT = 8000
MODAL_HOST_URL = "https://copev313--prefect-server-prefect-server.modal.run"
DEFAULT_WORK_POOL_NAME = "kiddy-pool"
MAX_CONCURRENCY = 12

app = modal.App("prefect-server")

image = (
    modal.Image.from_registry(
        f"prefecthq/prefect:{PREFECT_VERSION}-python{PYTHON_VERSION}",
    )
    .env(
        {
            "PREFECT_UI_API_URL": f"{MODAL_HOST_URL}/api",
            "PREFECT_UI_URL": f"{MODAL_HOST_URL}",
            "PREFECT_LOGGING_LOG_PRINTS": "True",
            "PREFECT_LOGGING_TO_API_BATCH_INTERVAL": "3",
            "PREFECT_SERVER_API_HOST": API_HOST,
            "PREFECT_SERVER_API_PORT": str(API_PORT),
            "PREFECT_SERVER_ANALYTICS_ENABLED": "False",
            "PREFECT_SERVER_CSRF_PROTECTION_ENABLED": "True",
            "PREFECT_TASKS_DEFAULT_NO_CACHE": "True",
            "PREFECT_SERVER_DATABASE_TIMEOUT": "20",
            "PREFECT_SERVER_DATABASE_CONNECTION_TIMEOUT": "10",
            "PREFECT_SERVER_TASKS_TAG_CONCURRENCY_SLOT_WAIT_SECONDS": "20",
            "PREFECT_TASKS_DEFAULT_RETRIES": "1",
            "PREFECT_TASKS_DEFAULT_RETRY_DELAY_SECONDS": "30",
            "PREFECT_DEPLOYMENTS_DEFAULT_WORK_POOL_NAME": DEFAULT_WORK_POOL_NAME,
        }
    )
    .pip_install(
        [
            "prefect-aws",
            "prefect-dbt",
            "prefect-dask",
        ]
    )
)

volume = modal.Volume.from_name("prefect-data", create_if_missing=True)


@app.function(
    image=image,
    volumes={"/root/.prefect": volume},
    secrets=[modal.Secret.from_name("prefect-auth-string")],
    min_containers=1,
    max_containers=4,
    timeout=30 * 60,  # 30 minutes
)
@modal.concurrent(max_inputs=MAX_CONCURRENCY)
@modal.web_server(port=API_PORT, startup_timeout=15)
def prefect_server():
    """Run the Prefect server via the CLI."""

    import os
    import subprocess

    if "PREFECT_API_AUTH_STRING" in os.environ:
        auth_string = os.environ["PREFECT_API_AUTH_STRING"]
        os.environ["PREFECT_SERVER_API_AUTH_STRING"] = auth_string
    else:
        raise ValueError("PREFECT_API_AUTH_STRING environment variable is not set.")

    subprocess.Popen(
        ["prefect", "server", "start"],
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )


def prefect_worker():
    """Run the Prefect worker with FastAPI and Uvicorn."""
    import os
    import subprocess

    if "PREFECT_API_AUTH_STRING" not in os.environ:
        raise ValueError("PREFECT_API_AUTH_STRING environment variable is not set.")

    cmd = ["prefect", "worker", "start"]
    subprocess.Popen(
        " ".join(cmd),
        shell=True,
        stderr=subprocess.STDOUT,
        stdout=subprocess.PIPE,
    )
