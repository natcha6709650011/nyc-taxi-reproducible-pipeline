import logging

from ingestion import run_ingestion
from validation import run_validation
from transformation import run_transformation
from integration import run_integration
from analysis import run_analysis


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)s | %(message)s"
)

def run_pipeline():
    """
    Run the entire NYC Taxi data pipeline.
    """
    logging.info("="*60)
    logging.info("Starting NYC Taxi data pipeline")
    logging.info("="*60)

    #Step 1: Ingestion
    logging.info("Step 1: Ingestion")
    ingestion_success = run_ingestion()

    if not ingestion_success:
        logging.error("Ingestion failed. Aborting pipeline.")
        return False

    logging.info("Step 1 completed successfully.")

    #Step 2: Validation
    logging.info("Step 2: Validation")
    validation_success = run_validation()

    if not validation_success:
        logging.error("Validation failed. Aborting pipeline.")
        return False

    logging.info("Step 2 completed successfully.")

    
    # Step 3: Transformation
    logging.info("Step 3: Transformation")
    try:
        transformation_results = run_transformation()
    except Exception:
        logging.exception("Transformation failed. Aborting pipeline.")
        return False

    logging.info("Step 3 completed successfully.")


    # Step 4: Integration
    logging.info("Step 4: Integration")
    try:
        integration_results = run_integration()
    except Exception:
        logging.exception("Integration failed. Aborting pipeline.")
        return False

    logging.info("Step 4 completed successfully.")


    # Step 5: Analysis
    logging.info("Step 5: Analysis")
    try:
        analysis_results = run_analysis()
    except Exception:
        logging.exception("Analysis failed. Aborting pipeline.")
        return False

    logging.info("Step 5 completed successfully.")

    logging.info("=" * 60)
    logging.info("NYC TAXI DATA PIPELINE COMPLETED SUCCESSFULLY")
    logging.info("=" * 60)

    return True


if __name__ == "__main__":
    pipeline_success = run_pipeline()

    if not pipeline_success:
        raise SystemExit(1)




    
