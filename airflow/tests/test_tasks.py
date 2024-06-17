import pytest

from airflow.operators.python import PythonOperator

from .utils import (
    get_tasks_of_dags,
    get_tasks_with_dags,
    tasks_op_kwargs_meet_func_args,
    tasks_op_kwargs_have_unexpected_keys,
    tasks_validate_op_kwargs_types_and_templates,
)


@pytest.mark.parametrize(
    "dag_id, task, fileloc",
    get_tasks_of_dags(),
    ids=[" ".join((x[1].task_id, x[2])) for x in get_tasks_of_dags()],
)
def test_dag_tasks_do_not_use_op_args(dag_id, task, fileloc):
    """
    test if tasks of PythonOperator class don't use op_arg param
    """
    if isinstance(task, PythonOperator):
        assert (
            not task.op_args
        ), f"Task {task.task_id} of {dag_id} DAG in {fileloc} uses not conventional 'op_args' parameter"


test_tasks_op_kwargs_meet_func_args = pytest.mark.parametrize(
    "dag_id, task, fileloc",
    get_tasks_of_dags(),
    ids=[" ".join((x[1].task_id, x[2])) for x in get_tasks_of_dags()],
)(tasks_op_kwargs_meet_func_args)


test_tasks_op_kwargs_have_unexpected_keys = pytest.mark.parametrize(
    "dag_id, task, fileloc",
    get_tasks_of_dags(),
    ids=[" ".join((x[1].task_id, x[2])) for x in get_tasks_of_dags()],
)(tasks_op_kwargs_have_unexpected_keys)


test_tasks_validate_op_kwargs_types_and_templates = pytest.mark.parametrize(
    "dag_id, dag, task, fileloc",
    get_tasks_with_dags(),
    ids=[" ".join((x[2].task_id, x[3])) for x in get_tasks_with_dags()],
)(tasks_validate_op_kwargs_types_and_templates)
