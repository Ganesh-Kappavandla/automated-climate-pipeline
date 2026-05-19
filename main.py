from pipeline_core.scrapers import ClimateDataPipeline

def run_system_engine():
    print("Initializing Automated Climate Data Pipeline Core...")
    pipeline = ClimateDataPipeline()
    success = pipeline.execute_transform_and_load()
    if success:
        print("Pipeline run completed cleanly. Data appended to output_data/ directory.")
    else:
        print("Pipeline execution failed. Review logs/pipeline_execution.log for diagnostic trace.")

if __name__ == "__main__":
    run_system_engine()  
