AIRFLOW_HOME=$(shell pwd)/airflow

all:
	@echo "see README.md"

.PHONY run-airflow:
run-airflow:
	@echo "Starting Airflow"
	NO_PROXY="*" AIRFLOW_HOME=$(AIRFLOW_HOME) airflow standalone

.PHONY run-streamlit:
run-streamlit:
	@echo "Starting Streamlit"
	streamlit run app.py
