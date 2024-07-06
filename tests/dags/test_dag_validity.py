from airflow.models import DagBag


def test_no_import_errors(monkeypatch) -> None:
    """
    Test that there are no import errors in the DAGs.

    This function sets environment variables for testing purposes and then checks if there are any import errors
    in the DAGs. It asserts that the number of import errors is 0 and the size of the DAG bag is 1.

    Args:
        monkeypatch: A monkeypatch object for modifying environment variables.

    Returns:
        None
    """
    monkeypatch.setenv("AIRFLOW_VAR_BUCKET", "test-bucket")
    monkeypatch.setenv("AIRFLOW_VAR_EMR_ID", "test-emr-id")

    dag_bag = DagBag()
    assert len(dag_bag.import_errors) == 0, "No Import Failures"
    assert dag_bag.size() == 1
