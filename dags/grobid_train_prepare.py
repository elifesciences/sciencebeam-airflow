from airflow.operators.bash_operator import BashOperator
from airflow.models import DAG

from sciencebeam_dag_ids import ScienceBeamDagIds

from sciencebeam_dag_utils import (
    get_default_args,
    add_dag_macro,
    create_trigger_next_task_dag_operator
)

from grobid_train_utils import (
    get_grobid_trainer_image,
    get_grobid_trainer_tools_image,
    create_grobid_train_validate_config_operation,
    DEFAULT_GROBID_TRAIN_FIELDS
)

from container_operators import ContainerRunOperator


DEFAULT_ARGS = get_default_args()


GET_DATA_TEMPLATE = (
    '''
    python -m sciencebeam_trainer_grobid_tools.download_source_files \
        {% for key, value in get_source_dataset_args(dag_run.conf.train.source_dataset).items() %} \
            "--{{ key }}={{ value }}" \
        {% endfor %} \
        --document-output-path "{{ dag_run.conf.train.grobid.dataset }}/pdf" \
        --document-output-filename-pattern "{name}.pdf.gz" \
        --target-output-path "{{ dag_run.conf.train.grobid.dataset }}/xml" \
        --target-output-filename-pattern "{document.name}.xml.gz" \
        {% if dag_run.conf.train.limit | default(false) %} \
            --limit "{{ dag_run.conf.train.limit }}" \
        {% endif %} \
        --debug \
        --threads 10
    '''
)


GENERATE_TRAINING_DATA_TEMPLATE = (
    '''
    bash -c '\
        download-dataset-pdf.sh "{{ dag_run.conf.train.grobid.dataset }}" "/tmp/dataset/pdf" \
        && generate-grobid-training-data.sh /tmp/dataset/pdf /tmp/dataset \
        && upload-dataset.sh /tmp/dataset "{{ dag_run.conf.train.grobid.dataset }}" \
    ' \
    '''
)


AUTO_ANNOTATE_HEADER_TEMPLATE = (
    '''
    python -m sciencebeam_trainer_grobid_tools.auto_annotate_header \
        --source-base-path "{{ dag_run.conf.train.grobid.dataset }}/header/corpus/tei-raw" \
        --output-path "{{ dag_run.conf.train.grobid.dataset }}/header/corpus/tei-auto" \
        --xml-path "{{ dag_run.conf.train.grobid.dataset }}/xml" \
        --xml-filename-regex '/(.*).header.tei.xml/\\1.xml/' \
        --fields "{{ dag_run.conf.train.get('fields') or DEFAULT_GROBID_TRAIN_FIELDS }}" \
        --no-preserve-tags
    '''
)


COPY_AUTO_ANNOTATED_HEADER_TEMPLATE = (
    '''
    gsutil -m cp \
        "{{ dag_run.conf.train.grobid.dataset }}/header/corpus/tei-auto/*.xml.gz" \
        "{{ dag_run.conf.train.grobid.dataset }}/header/corpus/tei/"
    '''
)


GENERATE_TARGET_XML_FILE_LIST_TEMPLATE = (
    '''
    gsutil -m ls \
        "{{ dag_run.conf.train.grobid.dataset }}/xml/*.xml.gz" \
        | sort \
        | gsutil cp - "{{ dag_run.conf.train.grobid.dataset }}/xml/file-list.lst"
    '''
)


GENERATE_HEADER_TEI_XML_FILE_LIST_TEMPLATE = (
    '''
    python -m sciencebeam_utils.tools.get_output_files \
        --source-base-path "{{ dag_run.conf.train.grobid.dataset }}/xml" \
        --source-file-list file-list.lst \
        --source-file-column=target_url \
        --output-base-path "{{ dag_run.conf.train.grobid.dataset }}/header/corpus/tei" \
        --output-file-suffix=.header.tei.xml.gz \
        --output-file-list \
            "{{ dag_run.conf.train.grobid.dataset }}/header/corpus/tei/file-list.lst" \
        --check
    '''
)


def get_source_dataset_args(source_dataset: dict) -> dict:
    source_file_list = source_dataset['source_file_list']
    return {
        'document-file-list': source_file_list,
        'document-file-column': source_dataset.get('source_file_column', 'source_url'),
        'target-file-list': source_dataset.get('target_file_list', source_file_list),
        'target-file-column': source_dataset.get('target_file_column', 'xml_url')
    }


def create_get_data_operator(dag, task_id='get_data'):
    add_dag_macro(dag, 'get_grobid_trainer_tools_image', get_grobid_trainer_tools_image)
    add_dag_macro(dag, 'get_source_dataset_args', get_source_dataset_args)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_grobid_trainer_tools_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "get-data") }}',
        preemptible=True,
        requests='cpu=100m,memory=256Mi',
        command=GET_DATA_TEMPLATE
    )


def create_generate_training_data_operator(dag, task_id='generate_training_data'):
    add_dag_macro(dag, 'get_grobid_trainer_image', get_grobid_trainer_image)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_grobid_trainer_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "training-data") }}',
        preemptible=True,
        requests='cpu=900m,memory=4Gi',
        command=GENERATE_TRAINING_DATA_TEMPLATE
    )


def create_auto_annotate_header_operator(dag, task_id='auto_annotate_header'):
    add_dag_macro(dag, 'get_grobid_trainer_tools_image', get_grobid_trainer_tools_image)
    add_dag_macro(dag, 'DEFAULT_GROBID_TRAIN_FIELDS', DEFAULT_GROBID_TRAIN_FIELDS)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_grobid_trainer_tools_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "auto-annotate") }}',
        preemptible=True,
        requests='cpu=300m,memory=800Mi',
        command=AUTO_ANNOTATE_HEADER_TEMPLATE
    )


def create_copy_auto_annotated_header_operator(dag, task_id='copy_auto_annotated_header'):
    return BashOperator(
        dag=dag,
        task_id=task_id,
        bash_command=COPY_AUTO_ANNOTATED_HEADER_TEMPLATE
    )


def create_generate_target_xml_file_list_operator(dag, task_id='generate_target_xml_file_list'):
    add_dag_macro(dag, 'get_grobid_trainer_tools_image', get_grobid_trainer_tools_image)
    return BashOperator(
        dag=dag,
        task_id=task_id,
        bash_command=GENERATE_TARGET_XML_FILE_LIST_TEMPLATE
    )


def create_generate_header_tei_xml_file_list_operator(
        dag, task_id='generate_header_tei_xml_file_list'):
    add_dag_macro(dag, 'get_grobid_trainer_tools_image', get_grobid_trainer_tools_image)
    return ContainerRunOperator(
        dag=dag,
        task_id=task_id,
        namespace='{{ dag_run.conf.namespace }}',
        image='{{ get_grobid_trainer_tools_image(dag_run.conf) }}',
        name='{{ generate_run_name(dag_run.conf.sciencebeam_release_name, "tei-xml-file-list") }}',
        preemptible=True,
        requests='cpu=100m,memory=128Mi',
        command=GENERATE_HEADER_TEI_XML_FILE_LIST_TEMPLATE,
    )


def create_dag():
    dag = DAG(
        dag_id=ScienceBeamDagIds.GROBID_TRAIN_PREPARE,
        default_args=DEFAULT_ARGS,
        schedule_interval=None
    )

    _ = (
        create_grobid_train_validate_config_operation(dag=dag)
        >> create_get_data_operator(dag=dag)
        >> create_generate_training_data_operator(dag=dag)
        >> create_auto_annotate_header_operator(dag=dag)
        >> create_copy_auto_annotated_header_operator(dag=dag)
        >> create_generate_target_xml_file_list_operator(dag=dag)
        >> create_generate_header_tei_xml_file_list_operator(dag=dag)
        >> create_trigger_next_task_dag_operator(dag=dag)
    )

    return dag


MAIN_DAG = create_dag()
