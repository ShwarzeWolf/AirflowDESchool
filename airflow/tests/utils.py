import inspect
import logging
import os
import re
from collections import defaultdict
from contextlib import contextmanager
from typing import Tuple, List, Dict, Union

from airflow import DAG, macros
from airflow.configuration import conf
from airflow.models import DagBag
from airflow.models.operator import Operator
from airflow.operators.python import PythonOperator
from airflow.plugins_manager import integrate_macros_plugins
from airflow.utils.timezone import utcnow

from jinja2.exceptions import UndefinedError
from pendulum import duration


APPROVED_TAGS = {}


_AUTO_PASSING_ARGS = {
    "conf",
    "conn",
    "dag",
    "dag_run",
    "task",
    "task_instance",
    "task_instance_key_str",
    "ti",
    "data_interval_start",
    "data_interval_end",
    "execution_date",
    "logical_date",
    "ds",
    "ds_nodash",
    "ts",
    "ts_nodash_with_tz",
    "ts_nodash",
    "next_execution_date",
    "next_ds",
    "next_ds_nodash",
    "yesterday_ds",
    "yesterday_ds_nodash",
    "tomorrow_ds",
    "tomorrow_ds_nodash",
    "prev_data_interval_start_success",
    "prev_data_interval_end_success",
    "prev_execution_date",
    "prev_execution_date_success",
    "prev_start_date_success",
    "prev_ds",
    "prev_ds_nodash",
    "inlets",
    "outlets",
    "params",
    "test_mode",
    "run_id",
    "macros",
    "var",
    "triggering_dataset_events",
    "args",
    "kwargs",
    "context",
}


class _AttrDict(defaultdict):
    __getattr__ = defaultdict.__getitem__

    def __init__(self, *args, **kwargs):
        super(_AttrDict, self).__init__(
            _AttrDict, *args, **kwargs
        )  # pylint: disable=super-with-arguments

    def __call__(self, *args, **kwargs):
        return _AttrDict()


def _strip_path_prefix(path) -> str:
    return os.path.relpath(path, os.environ.get("AIRFLOW_HOME"))


@contextmanager
def _suppress_logging(namespace):
    logger = logging.getLogger(namespace)
    old_value = logger.disabled
    logger.disabled = True
    try:
        yield
    finally:
        logger.disabled = old_value


def get_import_errors() -> List[Tuple]:
    """
    Generate a tuple for import errors in the dag bag
    """
    with _suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    # we prepend "(None,None)" to ensure that a test object is always created even if its a no op.
    return [(None, None)] + [
        (_strip_path_prefix(k), v.strip())
        for k, v in dag_bag.import_errors.items()
        if not "tests" in k
    ]


def get_dags() -> List[Tuple[str, DAG, str]]:
    """
    Generate a list of tuples of dag_id, <DAG objects> and fileloc in the DagBag
    """
    with _suppress_logging("airflow"):
        dag_bag = DagBag(include_examples=False)

    return [(k, v, _strip_path_prefix(v.fileloc)) for k, v in dag_bag.dags.items()]


def get_tasks_of_dags() -> List[Tuple[str, Operator, str]]:
    """
    Generate a tuple of dag_id, <Task objects> and fileloc in the DagBag
    """

    tasks = []
    get_task_list_for_dag = lambda dag: [
        (dag[0], task, dag[2]) for task in dag[1].tasks
    ]

    for dag in get_dags():
        tasks.extend(get_task_list_for_dag(dag))

    return tasks


def get_tasks_with_dags() -> List[Tuple[str, DAG, Operator, str]]:
    """
    Generate a tuple of dag_id, <DAG objects>, <Task objects> and fileloc in the DagBag
    """

    tasks = []
    get_task_list_for_dag = lambda dag: [
        (dag[0], dag[1], task, dag[2]) for task in dag[1].tasks
    ]

    for dag in get_dags():
        tasks.extend(get_task_list_for_dag(dag))

    return tasks


def _get_func_parameters(func) -> Dict[str, Dict[str, type]]:
    signature = inspect.signature(func)
    params = {
        "required": {
            name: param.annotation
            for name, param in signature.parameters.items()
            if name not in _AUTO_PASSING_ARGS and param.default == inspect._empty
        },
        "total": {
            name: param.annotation
            for name, param in signature.parameters.items()
            if name not in _AUTO_PASSING_ARGS
        },
    }
    return params


def tasks_op_kwargs_meet_func_args(dag_id, task, fileloc):
    """
    test if op_kwargs missing some arguments for python_callable
    """

    if not isinstance(task, PythonOperator):
        return

    required_func_args = _get_func_parameters(task.python_callable)["required"]
    task_op_kwargs = task.op_kwargs

    missed_args = set(required_func_args) - set(task_op_kwargs)
    assert not missed_args, (
        f"Task {task.task_id} of {dag_id} DAG in {fileloc} missing {missed_args} arguments needed for python_callable "
        f"{task.python_callable}, task op_kwargs -- {task_op_kwargs}"
    )


def tasks_op_kwargs_have_unexpected_keys(dag_id, task, fileloc):
    """
    test if op_kwargs pass some unexpected arguments for python_callable
    """

    if not isinstance(task, PythonOperator):
        return

    total_func_args = _get_func_parameters(task.python_callable)["total"]
    task_op_kwargs = task.op_kwargs

    unexpected_args = set(task_op_kwargs) - set(total_func_args)
    assert not unexpected_args, (
        f"Task {task.task_id} of {dag_id} DAG in {fileloc} has unexpected by python_callable {task.python_callable} "
        f"arguments: {unexpected_args}"
    )


def _is_templated_value(value) -> bool:
    if isinstance(value, str):
        return bool(re.match(r"\{\{.*?}}", value))
    return False


def _provide_context(task: Operator, dag: DAG) -> dict:
    integrate_macros_plugins()

    def get_triggering_events() -> Dict[str, list]:
        triggering_events: Dict[str, list] = {}
        for dataset in dag.dataset_triggers:
            triggering_events[dataset.uri] = []
        return triggering_events

    now = utcnow()
    now_iso = now.isoformat()
    date_iso = now.date().isoformat()
    date_nodash = date_iso.replace("-", "")
    yesterday = now - duration(days=1)
    yesterday_date_iso = yesterday.date().isoformat()
    yesterday_date_iso_nodash = yesterday_date_iso.replace("-", "")
    yesterday_date_with_tz_iso_nodash = yesterday_date_iso.replace("-", "").replace(
        ":", ""
    )
    tomorrow = now + duration(days=1)
    tomorrow_date_iso = tomorrow.date().isoformat()

    return {
        "conf": conf,
        "conn": _AttrDict(),
        "dag": dag,
        "dag_run": _AttrDict(),
        "task": task,
        "task_instance": _AttrDict(),
        "task_instance_key_str": "__".join((dag.dag_id, task.task_id, date_nodash)),
        "ti": _AttrDict(),
        "data_interval_start": now,
        "data_interval_end": now,
        "execution_date": now,
        "logical_date": now,
        "ds": date_iso,
        "ds_nodash": date_nodash,
        "ts": now_iso,
        "ts_nodash_with_tz": yesterday_date_with_tz_iso_nodash,
        "ts_nodash": yesterday_date_with_tz_iso_nodash.split(".")[0],
        "next_execution_date": now,
        "next_ds": date_iso,
        "next_ds_nodash": date_nodash,
        "yesterday_ds": yesterday_date_iso,
        "yesterday_ds_nodash": yesterday_date_iso_nodash,
        "tomorrow_ds": tomorrow_date_iso,
        "tomorrow_ds_nodash": tomorrow_date_iso.replace("-", ""),
        "prev_data_interval_start_success": yesterday,
        "prev_data_interval_end_success": yesterday,
        "prev_execution_date": yesterday,
        "prev_execution_date_success": yesterday,
        "prev_start_date_success": yesterday,
        "prev_ds": yesterday_date_iso,
        "prev_ds_nodash": yesterday_date_iso_nodash,
        "inlets": task.inlets,
        "outlets": task.outlets,
        "params": task.params,
        "test_mode": False,
        "run_id": "__".join(("manual", now_iso)),
        "macros": macros,
        "var": {"json": None, "value": None},
        "triggering_dataset_events": get_triggering_events(),
    }


def _validate_templated_value(value: str, task: Operator, dag: DAG) -> Tuple[bool, str]:
    context = _provide_context(task=task, dag=dag)
    try:
        task.render_template(value, context)
        return True, ""
    except UndefinedError as err:
        return False, err.message


def tasks_validate_op_kwargs_types_and_templates(dag_id, dag, task, fileloc):
    """
    test if op_kwargs pass arguments to python_callable with correct types or arguments have valid jinja template
    """

    if not isinstance(task, PythonOperator):
        return

    func_args = _get_func_parameters(task.python_callable)["total"]
    task_op_kwargs = task.op_kwargs

    for key, value in task_op_kwargs.items():
        if key in _AUTO_PASSING_ARGS:
            continue

        arg_type = func_args.get(key, type(None))

        if _is_templated_value(value):
            is_valid_template = _validate_templated_value(value, task, dag)
            assert is_valid_template[0], (
                f"Task {task.task_id} of {dag_id} DAG in {fileloc} has invalid templated value in "
                f"op_kwargs: {is_valid_template[1]}"
            )

        else:

            def get_origin_type(arg_type):
                while arg_type.__module__ == "typing":
                    origin = arg_type.__origin__
                    if origin == Union:
                        arg_type = arg_type.__reduce__()[1][1][0]
                    else:
                        arg_type = origin
                return arg_type

            arg_type = get_origin_type(arg_type)

            assert isinstance(value, arg_type) or isinstance(
                arg_type, inspect._empty
            ), (
                f"The type of '{key}' arg of the op_kwargs param of the task {task.task_id} of {dag_id} DAG "
                f"in {fileloc} is unexpected. Expected type is '{arg_type}', {type(value)} is provided."
            )
