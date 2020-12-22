#!/usr/bin/make -f


ifndef SCIENCEBEAM_NAMESPACE
	override SCIENCEBEAM_NAMESPACE = dev
endif


DOCKER_COMPOSE_DEV = docker-compose
DOCKER_COMPOSE_CI = docker-compose -f docker-compose.yml
DOCKER_COMPOSE = $(DOCKER_COMPOSE_DEV)

VENV = venv
PIP = $(VENV)/bin/pip
PYTHON = PYTHONPATH=dags $(VENV)/bin/python


GCP_PROJECT = elife-ml

# optional charts dir
SCIENCEBEAM_CHARTS_DIR = ../sciencebeam-charts

SCIENCEBEAM_ADHOC_RELEASE_NAME = sciencebeam-adhoc-$(shell date +%s -u)--$(SCIENCEBEAM_NAMESPACE)
SCIENCEBEAM_IMAGE_REPO = elifesciences/sciencebeam
SCIENCEBEAM_IMAGE_TAG = 0.0.8
SCIENCEBEAM_ARGS = --no-grobid-xslt
GROBID_IMAGE_REPO = lfoppiano/grobid
GROBID_IMAGE_TAG = 0.5.3
GROBID_MODEL_NAME = grobid-tei-$(GROBID_IMAGE_TAG)

MODEL_NAME = $(GROBID_MODEL_NAME)

LIMIT = 5
SAMPLE_DATASET_NAME = pmc-sample-1943-cc-by-subset
SAMPLE_DATASET_NAME_TRAIN = $(SAMPLE_DATASET_NAME)-train
SAMPLE_DATASET_NAME_VALIDATION = $(SAMPLE_DATASET_NAME)-validation
SAMPLE_DATA_PATH = gs://sciencebeam-samples/$(SAMPLE_DATASET_NAME)
SAMPLE_FILE_LIST = file-list.tsv
SAMPLE_TRAIN_FILE_LIST = file-list-train.tsv
SAMPLE_VALIDATION_FILE_LIST = file-list-validation.tsv
BASE_OUTPUT_DATA_PATH = $(SAMPLE_DATA_PATH)-results/$(SCIENCEBEAM_NAMESPACE)
GROBID_OUTPUT_DATA_PATH = $(BASE_OUTPUT_DATA_PATH)/$(GROBID_MODEL_NAME)
OUTPUT_DATA_PATH = $(BASE_OUTPUT_DATA_PATH)/$(MODEL_NAME)
OUTPUT_FILE_LIST = file-list.lst
OUTPUT_VALIDATION_FILE_LIST = file-list-validation-$(LIMIT).lst
OUTPUT_TRAIN_FILE_LIST = file-list-train-$(LIMIT).lst
OUTPUT_SUFFIX = .xml.gz
EVAL_OUTPUT_PATH = $(OUTPUT_DATA_PATH)/evaluation-results
RESUME = true

TRAIN_DATASET = $(SAMPLE_DATASET_NAME)

TRAIN_LIMIT = 10
TRAIN_DATA_PATH = $(SAMPLE_DATA_PATH)
BASE_TRAIN_OUTPUT_PATH = $(TRAIN_DATA_PATH)-models/$(SCIENCEBEAM_NAMESPACE)
GROBID_BASE_OUTPUT_PATH = $(BASE_TRAIN_OUTPUT_PATH)/grobid-$(GROBID_IMAGE_TAG)/limit-$(TRAIN_LIMIT)
GROBID_DATASET_PATH = $(GROBID_BASE_OUTPUT_PATH)/dataset
GROBID_MODELS_PATH = $(GROBID_BASE_OUTPUT_PATH)/models
GROBID_TRAINED_MODEL_IMAGE_REPO = gcr.io/$(GCP_PROJECT)/grobid-trained-model--$(SCIENCEBEAM_NAMESPACE)
GROBID_TRAINED_MODEL_IMAGE_TAG = $(GROBID_IMAGE_TAG)-$(TRAIN_DATASET)-$(TRAIN_LIMIT)
GROBID_TRAINED_MODEL_IMAGE = $(GROBID_TRAINED_MODEL_IMAGE_REPO):$(GROBID_TRAINED_MODEL_IMAGE_TAG)
GROBID_TRAINED_MODEL_NAME = grobid-tei-$(GROBID_TRAINED_MODEL_IMAGE_TAG)
GROBID_CROSSREF_ENABLED = false

AUTOCUT_TAG = 40ff412896fc3b5077803c759beaf92f3e8970cb
AUTOCUT_TRAINED_MODEL_IMAGE_REPO = gcr.io/$(GCP_PROJECT)/sciencebeam-autocut-trained-model--$(SCIENCEBEAM_NAMESPACE)
AUTOCUT_TRAINED_MODEL_IMAGE_TAG = $(AUTOCUT_TAG)-$(TRAIN_DATASET)-$(TRAIN_LIMIT)
AUTOCUT_TRAINED_MODEL_IMAGE = $(AUTOCUT_TRAINED_MODEL_IMAGE_REPO):$(AUTOCUT_TRAINED_MODEL_IMAGE_TAG)
AUTOCUT_TRAINED_MODEL_NAME = autocut-$(AUTOCUT_TRAINED_MODEL_IMAGE_TAG)
AUTOCUT_TRAINED_MODEL_PATH = $(BASE_TRAIN_OUTPUT_PATH)/autocut-$(AUTOCUT_TAG)/limit-$(TRAIN_LIMIT)/model.pkl
AUTOCUT_SCIENCEBEAM_ARGS =

AUTOCUT_OUTPUT_DATA_PATH = $(BASE_OUTPUT_DATA_PATH)/$(AUTOCUT_TRAINED_MODEL_NAME)
AUTOCUT_EVAL_OUTPUT_PATH = $(AUTOCUT_OUTPUT_DATA_PATH)/evaluation-results

SCIENCEBEAM_CHARTS_COMMIT = $(shell bash -c 'source .env && echo $$SCIENCEBEAM_CHARTS_COMMIT')

FIELDS =
MEASURES =
SCORING_TYPE_OVERRIDES =

WORKER_COUNT = 10
REPLICA_COUNT = 0

ARGS =


venv-clean:
	@if [ -d "$(VENV)" ]; then \
		rm -rf "$(VENV)"; \
	fi


venv-create:
	python3 -m venv $(VENV)


dev-install:
	$(PIP) install --disable-pip-version-check -r requirements.build.txt

	export AIRFLOW_GPL_UNIDECODE=yes
	$(PIP) install --disable-pip-version-check -r requirements.prereq.txt

	$(PIP) install --disable-pip-version-check -e .

	$(PIP) install --disable-pip-version-check -r requirements.dev.txt


dev-venv: venv-create dev-install


dev-flake8:
	$(PYTHON) -m flake8 dags tests


dev-pylint:
	$(PYTHON) -m pylint dags tests


dev-lint: dev-flake8 dev-pylint


dev-pytest:
	GOOGLE_CLOUD_PROJECT=dummy \
	SCIENCEBEAM_CONFIG_DATA_PATH=gs://dummy \
	SCIENCEBEAM_WATCH_INTERVAL=1000 \
	$(PYTHON) -m pytest -p no:cacheprovider $(ARGS)


dev-watch:
	GOOGLE_CLOUD_PROJECT=dummy \
	SCIENCEBEAM_CONFIG_DATA_PATH=gs://dummy \
	SCIENCEBEAM_WATCH_INTERVAL=1000 \
	$(PYTHON) -m pytest_watch --verbose --ext=.py,.xsl -- -p no:cacheprovider -k 'not slow' $(ARGS)


dev-watch-slow:
	GOOGLE_CLOUD_PROJECT=dummy \
	SCIENCEBEAM_CONFIG_DATA_PATH=gs://dummy \
	SCIENCEBEAM_WATCH_INTERVAL=1000 \
	$(PYTHON) -m pytest_watch --verbose --ext=.py,.xsl -- -p no:cacheprovider $(ARGS)


dev-test: dev-lint dev-pytest


helm-charts-clone:
	@if [ ! -d 'helm' ]; then \
		git clone https://github.com/elifesciences/sciencebeam-charts.git helm; \
	fi
	cd helm && git fetch && git checkout $(SCIENCEBEAM_CHARTS_COMMIT)


helm-charts-copy:
	mkdir -p ./helm
	cp -r --dereference "$(SCIENCEBEAM_CHARTS_DIR)"/* ./helm/


helm-charts-get:
	@if [ -d "$(SCIENCEBEAM_CHARTS_DIR)" ]; then \
		$(MAKE) helm-charts-copy; \
	else \
		$(MAKE) helm-charts-clone; \
	fi


helm-charts-update: helm-charts-get


build: helm-charts-update
	$(DOCKER_COMPOSE) build airflow-image


build-init:
	$(DOCKER_COMPOSE) build init


build-dev: helm-charts-update
	# only dev compose file has "init" service defined
	@if [ "$(DOCKER_COMPOSE)" = "$(DOCKER_COMPOSE_DEV)" ]; then \
		$(MAKE) build-init; \
	fi
	$(DOCKER_COMPOSE) build airflow-dev-base-image airflow-dev


test: build-dev
	$(DOCKER_COMPOSE) run --rm airflow-dev /bin/sh ./project-tests.sh


watch: build-dev
	$(DOCKER_COMPOSE) run --rm airflow-dev python -m pytest_watch


deploy-sciencebeam:
	$(DOCKER_COMPOSE) run --rm --no-deps --entrypoint='' airflow-webserver \
		python -m sciencebeam_airflow.tools.deploy_sciencebeam $(ARGS)


airflow-initdb:
	$(DOCKER_COMPOSE) run --rm  airflow-webserver initdb


start: helm-charts-update
	$(eval SERVICE_NAMES = $(shell docker-compose config --services | grep -v 'airflow-dev'))
	$(DOCKER_COMPOSE) up --build -d --scale airflow-worker=2 $(SERVICE_NAMES)


stop:
	$(DOCKER_COMPOSE) down


restart:
	$(DOCKER_COMPOSE) restart


logs: 
	$(DOCKER_COMPOSE) logs -f


clean:
	$(DOCKER_COMPOSE) down -v


web-shell:
	$(DOCKER_COMPOSE) run --no-deps airflow-webserver bash


web-exec:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint bash


worker-shell:
	$(DOCKER_COMPOSE) run --no-deps airflow-worker bash


worker-exec:
	$(DOCKER_COMPOSE) exec airflow-worker /entrypoint bash


dags-list:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint list_dags


dags-unpause:
	$(eval DAG_IDS = $(shell \
		$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint list_dags \
		| grep -P "^(sciencebeam_|grobid_)\S+" \
	))
	@echo DAG_IDS=$(DAG_IDS)
	@for DAG_ID in $(DAG_IDS); do \
		echo DAG_ID=$${DAG_ID}; \
		$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint unpause $${DAG_ID}; \
	done


tasks-list:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint list_tasks $(ARGS)


trigger-gs-list-buckets:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint trigger_dag gs_list_buckets


trigger-kube-list-pods:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint trigger_dag kube_list_pods


trigger-helm-version:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint trigger_dag helm_version


.run_id:
	$(eval CREATED_TS = $(shell date --iso-8601=seconds -u))
	$(eval RUN_ID = make__$(CREATED_TS)_$(SAMPLE_DATASET_NAME)_$(MODEL_NAME))


.sciencebeam-common-conf:
	$(eval export SCIENCEBEAM_COMMON_CONF = $(shell echo ' \
		"run_name": "$(SAMPLE_DATASET_NAME)_$(MODEL_NAME)", \
		"sciencebeam_release_name": "$(SCIENCEBEAM_ADHOC_RELEASE_NAME)", \
		"namespace": "$(SCIENCEBEAM_NAMESPACE)" \
	'))


.sciencebeam-convert-shared-conf:
	$(eval export SCIENCEBEAM_CONVERT_SHARED_CONF = $(shell echo ' \
		"dataset_name": "$(SAMPLE_DATASET_NAME_VALIDATION)", \
		"source_data_path": "$(SAMPLE_DATA_PATH)", \
		"source_file_list": "$(SAMPLE_VALIDATION_FILE_LIST)", \
		"output_data_path": "$(OUTPUT_DATA_PATH)", \
		"output_file_list": "$(OUTPUT_VALIDATION_FILE_LIST)", \
		"output_suffix": "$(OUTPUT_SUFFIX)", \
		"eval_output_path": "$(EVAL_OUTPUT_PATH)", \
		"resume": $(RESUME), \
		"limit": "$(LIMIT)" \
	'))


.sciencebeam-convert-eval-conf: .run_id .sciencebeam-common-conf .sciencebeam-convert-shared-conf
	$(eval export SCIENCEBEAM_CONVERT_EVAL_CONF = $(shell echo '{ \
		$(SCIENCEBEAM_COMMON_CONF), \
		$(SCIENCEBEAM_CONVERT_SHARED_CONF), \
		"config": { \
			"convert": { \
				"worker_count": "$(WORKER_COUNT)", \
				"replica_count": "$(REPLICA_COUNT)" \
			}, \
			"evaluate": { \
				"fields": "$(FIELDS)", \
				"measures": "$(MEASURES)", \
				"scoring_type_overrides": "$(SCORING_TYPE_OVERRIDES)" \
			} \
		}, \
		"model_name": "$(MODEL_NAME)", \
		"model": { \
			"name": "$(MODEL_NAME)", \
			"sciencebeam_image": "$(SCIENCEBEAM_IMAGE_REPO):$(SCIENCEBEAM_IMAGE_TAG)", \
			"sciencebeam_args": "$(SCIENCEBEAM_ARGS)", \
			"grobid_image": "$(GROBID_IMAGE_REPO):$(GROBID_IMAGE_TAG)" \
		}, \
		"tasks": [ \
			"sciencebeam_convert", \
			"sciencebeam_evaluate", \
			"sciencebeam_evaluation_results_to_bq" \
		] \
	}'))
	@echo SCIENCEBEAM_CONVERT_EVAL_CONF="$$SCIENCEBEAM_CONVERT_EVAL_CONF"
	@echo "$$SCIENCEBEAM_CONVERT_EVAL_CONF" | jq .


trigger-sciencebeam-convert: .run_id .sciencebeam-convert-eval-conf
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_CONVERT_EVAL_CONF" \
		sciencebeam_convert


trigger-sciencebeam-evaluate: .run_id .sciencebeam-convert-eval-conf
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_CONVERT_EVAL_CONF" \
		sciencebeam_evaluate


trigger-sciencebeam-evaluation-results-to-bq: .run_id .sciencebeam-convert-eval-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_CONVERT_EVAL_CONF" \
		sciencebeam_evaluation_results_to_bq


.grobid-train-conf: .sciencebeam-common-conf .sciencebeam-convert-shared-conf
	$(eval export SCIENCEBEAM_TRAIN_CONF = $(shell echo '{ \
		$(SCIENCEBEAM_COMMON_CONF), \
		$(SCIENCEBEAM_CONVERT_SHARED_CONF), \
		"model_name": "$(GROBID_TRAINED_MODEL_NAME)", \
		"model": { \
			"name": "$(GROBID_TRAINED_MODEL_NAME)", \
			"sciencebeam_image": "$(SCIENCEBEAM_IMAGE_REPO):$(SCIENCEBEAM_IMAGE_TAG)", \
			"sciencebeam_args": "$(SCIENCEBEAM_ARGS)", \
			"grobid_image": "$(GROBID_TRAINED_MODEL_IMAGE)" \
		}, \
		"gcp_project": "$(GCP_PROJECT)", \
		"train": { \
			"source_dataset": { \
				"name": "$(SAMPLE_DATASET_NAME_TRAIN)", \
				"subset_name": "train", \
				"source_file_list": "$(SAMPLE_DATA_PATH)/$(SAMPLE_TRAIN_FILE_LIST)" \
			}, \
			"limit": "$(TRAIN_LIMIT)", \
			"grobid": { \
				"dataset": "$(GROBID_DATASET_PATH)", \
				"models": "$(GROBID_MODELS_PATH)", \
				"source_image": "$(GROBID_IMAGE_REPO):$(GROBID_IMAGE_TAG)", \
				"output_image": "$(GROBID_TRAINED_MODEL_IMAGE)" \
			} \
		}, \
		"tasks": [ \
			"grobid_train_prepare", \
			"grobid_train_evaluate_grobid_train_tei", \
			"grobid_train_model", \
			"grobid_build_image", \
			"sciencebeam_convert", \
			"sciencebeam_evaluate", \
			"sciencebeam_evaluation_results_to_bq", \
			"grobid_train_evaluate_source_dataset" \
		] \
	}'))
	@echo SCIENCEBEAM_TRAIN_CONF="$$SCIENCEBEAM_TRAIN_CONF"
	@echo "$$SCIENCEBEAM_TRAIN_CONF" | jq .


trigger-grobid-train-prepare: .run_id .grobid-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_TRAIN_CONF" \
		grobid_train_prepare


trigger-grobid-train-evaluate: .run_id .grobid-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_TRAIN_CONF" \
		grobid_train_evaluate


trigger-grobid-train-model: .run_id .grobid-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_TRAIN_CONF" \
		grobid_train_model


trigger-grobid-build-image: .run_id .grobid-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_TRAIN_CONF" \
		grobid_build_image


trigger-grobid-train-evaluate-source-dataset: .run_id .grobid-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_TRAIN_CONF" \
		grobid_train_evaluate_source_dataset


.sciencebeam-autocut-train-conf: .sciencebeam-common-conf .sciencebeam-convert-shared-conf
	$(eval export SCIENCEBEAM_AUTOCUT_TRAIN_CONF = $(shell echo '{ \
		$(SCIENCEBEAM_COMMON_CONF), \
		"dataset_name": "$(SAMPLE_DATASET_NAME_VALIDATION)", \
		"source_data_path": "$(SAMPLE_DATA_PATH)", \
		"source_file_list": "$(SAMPLE_VALIDATION_FILE_LIST)", \
		"output_data_path": "$(AUTOCUT_OUTPUT_DATA_PATH)", \
		"output_file_list": "$(OUTPUT_VALIDATION_FILE_LIST)", \
		"output_suffix": "$(OUTPUT_SUFFIX)", \
		"eval_output_path": "$(AUTOCUT_EVAL_OUTPUT_PATH)", \
		"resume": $(RESUME), \
		"limit": "$(LIMIT)", \
		"model_name": "$(AUTOCUT_TRAINED_MODEL_NAME)", \
		"model": { \
			"name": "$(AUTOCUT_TRAINED_MODEL_NAME)", \
			"chart_args": { \
				"sciencebeam.args": "$(AUTOCUT_SCIENCEBEAM_ARGS)", \
				"grobid.enabled": "true", \
				"grobid.image.repository": "$(GROBID_IMAGE_REPO)", \
				"grobid.image.tag": "$(GROBID_IMAGE_TAG)", \
				"grobid.warmup.enabled": "true", \
				"grobid.crossref.enabled": "$(GROBID_CROSSREF_ENABLED)", \
				"sciencebeam.pipeline": "grobid\\\\,sciencebeam_autocut", \
				"sciencebeam_autocut.enabled": "true", \
				"sciencebeam-autocut.image.repository": "$(AUTOCUT_TRAINED_MODEL_IMAGE_REPO)", \
				"sciencebeam-autocut.image.tag": "$(AUTOCUT_TRAINED_MODEL_IMAGE_TAG)" \
			} \
		}, \
		"gcp_project": "$(GCP_PROJECT)", \
		"train": { \
			"source_dataset": { \
				"name": "$(SAMPLE_DATASET_NAME_TRAIN)", \
				"subset_name": "train", \
				"source_file_list": "$(SAMPLE_DATA_PATH)/$(SAMPLE_TRAIN_FILE_LIST)" \
			}, \
			"limit": "$(TRAIN_LIMIT)", \
			"autocut": { \
				"input_model": { \
					"name": "$(GROBID_MODEL_NAME)", \
					"sciencebeam_image": "$(SCIENCEBEAM_IMAGE_REPO):$(SCIENCEBEAM_IMAGE_TAG)", \
					"sciencebeam_args": "$(SCIENCEBEAM_ARGS)", \
					"grobid_image": "$(GROBID_IMAGE_REPO):$(GROBID_IMAGE_TAG)" \
				}, \
				"input_source": { \
					"xpath": "tei:teiHeader/tei:fileDesc/tei:titleStmt/tei:title", \
					"file_list": "$(GROBID_OUTPUT_DATA_PATH)/$(OUTPUT_TRAIN_FILE_LIST)" \
				}, \
				"output_image": "$(AUTOCUT_TRAINED_MODEL_IMAGE)", \
				"model": "$(AUTOCUT_TRAINED_MODEL_PATH)" \
			} \
		}, \
		"tasks": [ \
			"sciencebeam_autocut_convert_training_data", \
			"sciencebeam_autocut_train_model", \
			"sciencebeam_autocut_build_image", \
			"sciencebeam_convert", \
			"sciencebeam_evaluate", \
			"sciencebeam_evaluation_results_to_bq", \
			"grobid_train_evaluate_source_dataset" \
		] \
	}'))
	@echo SCIENCEBEAM_AUTOCUT_TRAIN_CONF="$$SCIENCEBEAM_AUTOCUT_TRAIN_CONF"
	@echo "$$SCIENCEBEAM_AUTOCUT_TRAIN_CONF" | jq .


trigger-sciencebeam-autocut-convert-training-data: .run_id .sciencebeam-autocut-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_AUTOCUT_TRAIN_CONF" \
		sciencebeam_autocut_convert_training_data


trigger-sciencebeam-autocut-train-model: .run_id .sciencebeam-autocut-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_AUTOCUT_TRAIN_CONF" \
		sciencebeam_autocut_train_model


trigger-sciencebeam-autocut-build-image: .run_id .sciencebeam-autocut-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_AUTOCUT_TRAIN_CONF" \
		sciencebeam_autocut_build_image


trigger-sciencebeam-autocut-convert-and-evaluate: .run_id .sciencebeam-autocut-train-conf
	docker-compose exec airflow-webserver /entrypoint trigger_dag \
		--run_id "$(RUN_ID)" \
		--conf "$$SCIENCEBEAM_AUTOCUT_TRAIN_CONF" \
		sciencebeam_convert


trigger-sciencebeam-watch:
	$(DOCKER_COMPOSE) exec airflow-webserver /entrypoint trigger_dag sciencebeam_watch_experiments


ci-build-and-test:
	make DOCKER_COMPOSE="$(DOCKER_COMPOSE_CI)" build test


ci-clean:
	$(DOCKER_COMPOSE_CI) down -v
