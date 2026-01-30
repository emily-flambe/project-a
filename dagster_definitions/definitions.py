from dagster import (
    asset,
    sensor,
    RunRequest,
    SensorEvaluationContext,
    Definitions,
    define_asset_job,
    DefaultSensorStatus,
)
import datetime


@asset
def healthcheck_asset():
    """Simple healthcheck asset that logs current time."""
    now = datetime.datetime.now().isoformat()
    return f"Healthcheck OK at {now}"


healthcheck_job = define_asset_job(
    name="healthcheck_job",
    selection=[healthcheck_asset],
)


@sensor(
    job=healthcheck_job,
    minimum_interval_seconds=300,  # 5 minutes
    default_status=DefaultSensorStatus.RUNNING,
)
def healthcheck_sensor(context: SensorEvaluationContext):
    """Sensor that triggers healthcheck every 5 minutes."""
    context.log.info("Healthcheck sensor tick")
    return RunRequest(run_key=f"healthcheck-{datetime.datetime.now().isoformat()}")


defs = Definitions(
    assets=[healthcheck_asset],
    jobs=[healthcheck_job],
    sensors=[healthcheck_sensor],
)
