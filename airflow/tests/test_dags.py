import pytest

from .utils import get_import_errors, get_dags, APPROVED_TAGS


@pytest.mark.parametrize(
    "rel_path, rv", get_import_errors(), ids=[x[0] for x in get_import_errors()]
)
def test_file_imports(rel_path, rv):
    """Test for import errors on a file"""
    if rel_path and rv:
        raise Exception(
            f"{rel_path} failed to import with message \n {rv}"
        )  # pylint: disable=broad-exception-raised


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_catchup_false(dag_id, dag, fileloc):
    """
    test if all DAGs contain a task
    """
    assert not dag.catchup, f"{dag_id} in {fileloc} has 'catchup=True'"


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_tags(dag_id, dag, fileloc):
    """
    test if a DAG is tagged and if those tags are in approved tags
    """
    assert dag.tags, f"{dag_id} in {fileloc} has no tags"
    if APPROVED_TAGS:
        assert (
            not set(dag.tags) - APPROVED_TAGS
        ), f"{dag_id} in {fileloc} has tags that aren't in list of approved tags"


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_tasks(dag_id, dag, fileloc):
    """
    test if all DAGs contain a task
    """
    assert dag.tasks, f"{dag_id} in {fileloc} has no tasks"


@pytest.mark.parametrize(
    "dag_id, dag, fileloc", get_dags(), ids=[x[2] for x in get_dags()]
)
def test_dag_has_defined_dependencies(dag_id, dag, fileloc):
    """
    test if dependencies of tasks of all DAGs are defined
    """
    assert (
        len(dag.tasks) == 1 or dag.roots != dag.leaves
    ), f"{dag_id} in {fileloc} has no defined tasks dependencies"
