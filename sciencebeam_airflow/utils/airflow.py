import logging
from inspect import getmembers

from airflow.models import DAG


LOGGER = logging.getLogger(__name__)


def add_dag_macro(dag: DAG, macro_name: str, macro_fn: callable):
    if not dag.user_defined_macros:
        dag.user_defined_macros = {}
    dag.user_defined_macros[macro_name] = macro_fn


def add_dag_macros(dag: DAG, macros: object):
    LOGGER.debug('adding macros: %s', macros)
    for name, value in getmembers(macros):
        if callable(value):
            LOGGER.debug('adding macro: %s', name)
            add_dag_macro(dag, name, value)
        else:
            LOGGER.debug('not adding macro, not callable: %s -> %s', name, value)
