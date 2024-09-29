import datetime
import logging

from streaming_pipeline import initialize
from streaming_pipeline.flow import build as flow_builder
import traceback

def build_flow(
    env_file_path: str = ".env",
    logging_config_path: str = "logging.yaml",
    model_cache_dir: str = None,
    latest_n_days: float = 0.1,
    debug: bool = False,
):
    """
    Builds a Bytewax flow for batch processing of news data from Alpaca.

    Args:
        env_file_path (str): Path to the environment file.
        logging_config_path (str): Path to the logging configuration file.
        model_cache_dir (str): Path to the directory where the model cache is stored.
        latest_n_days (float): Number of days to extract news from.
        debug (bool): Whether to run the flow in debug mode.

    Returns:
        flow (prefect.Flow): The Bytewax flow for batch processing of news data.
    """

    try:
        # Initializes the logger and environment variables.
        initialize(logging_config_path=logging_config_path, env_file_path=env_file_path)

        logger = logging.getLogger(__name__)

        to_datetime = datetime.datetime.now()
        from_datetime = to_datetime - datetime.timedelta(days=latest_n_days)
        logger.info(
            f"Extracting news from {from_datetime} to {to_datetime} [n_days={latest_n_days}]"
        )

        flow = flow_builder(
            is_batch=True,
            from_datetime=from_datetime,
            to_datetime=to_datetime,
            model_cache_dir=model_cache_dir,
            debug=debug,
            download_needed=False,  # Alpaca news provides content with event
            date_field="updated_at" # date field for Alpaca news
        )

    except Exception as e:
        logging.error(traceback.format_exc())

    return flow

# python command
# python -m bytewax.run "run_batch:build_flow(latest_n_days=1)"
# python -m bytewax.run "run_batch:build_flow(latest_n_days=1, debug=True)"