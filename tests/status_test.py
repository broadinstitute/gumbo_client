from gumbo_client import status
from datetime import datetime


def test_get_screen_status():
    past_date = datetime(2020, 1, 1)
    future_date = datetime(2050, 1, 1)
    # Complete
    assert status.get_screen_status(status=None, screener_qc="PASS", cds_qc="PASS", consortium_release_date=past_date) == status.Status.complete 
    assert status.get_screen_status(status="Done", screener_qc="PASS", cds_qc="PASS", consortium_release_date=past_date) == status.Status.complete 
    # Failed
    assert status.get_screen_status(status="Terminal Fail", screener_qc=None, cds_qc=None, consortium_release_date=None) == status.Status.failed 
    assert status.get_screen_status(status=None, screener_qc="PASS", cds_qc="something went wrong", consortium_release_date=None) == status.Status.failed 
    assert status.get_screen_status(status=None, screener_qc="FAIL", cds_qc=None, consortium_release_date=None) == status.Status.failed 
    # In Progress
    assert status.get_screen_status(status="Done", screener_qc="PASS", cds_qc="PASS", consortium_release_date=None) == status.Status.in_progress 
    assert status.get_screen_status(status="Done", screener_qc="PASS", cds_qc="PASS", consortium_release_date=future_date) == status.Status.in_progress 
    assert status.get_screen_status(status=None, screener_qc="PASS", cds_qc=None, consortium_release_date=future_date) == status.Status.in_progress 
    assert status.get_screen_status(status="In Progress", screener_qc=None, cds_qc=None, consortium_release_date=future_date) == status.Status.in_progress
    assert status.get_screen_status(status=None, screener_qc="PASS", cds_qc="missing CN data;", consortium_release_date=future_date) == status.Status.in_progress 
    # No status
    assert status.get_screen_status(status=None, screener_qc=None, cds_qc=None, consortium_release_date=None) == None


def test_get_omics_status():
    past_date = datetime(2020, 1, 1)
    # Complete
    assert status.get_omics_status(profile_status="Complete", main_sequencing_id="SomeIdVal", blacklist=None, consortium_release_date=past_date) == status.Status.complete
    # In Progress
    assert status.get_omics_status(profile_status="In Progress", main_sequencing_id=None, blacklist=None, consortium_release_date=None) == status.Status.in_progress
    # No Status
    assert status.get_omics_status(profile_status=None, main_sequencing_id=None, blacklist=None, consortium_release_date=None) == None


def test_get_screen_failure_details():
    # Refers to the substatus for terminal fails
    actual = status.get_screen_failure_details(
        screener_qc=None, cds_qc=None, screener_status="Terminal Fail", substatus="04 - Pooled screen queue"
    )
    expected = status.ScreenFailureDetails("Fail Pre-Screen QC", False)
    assert actual.failure_type == expected.failure_type
    assert actual.failed_post_data_gen == expected.failed_post_data_gen

    # Fingerprinting fails
    actual = status.get_screen_failure_details(
        screener_qc="FAIL - Fingerprinting", cds_qc=None, screener_status="Done", substatus="something"
    )
    expected = status.ScreenFailureDetails("Fail Fingerprinting", True)
    assert actual.failure_type == expected.failure_type
    assert actual.failed_post_data_gen == expected.failed_post_data_gen

    # Other Post screen fails
    actual = status.get_screen_failure_details(
        screener_qc="FAIL - Some other reason", cds_qc=None, screener_status="Done", substatus="something"
    )
    expected = status.ScreenFailureDetails("Post-Screen Fail - Other", True)
    assert actual.failure_type == expected.failure_type
    assert actual.failed_post_data_gen == expected.failed_post_data_gen

    # Failed CDS QC
    actual = status.get_screen_failure_details(
        screener_qc="PASS", cds_qc="FAIL", screener_status="Done", substatus=None
    )
    expected = status.ScreenFailureDetails("Fail Data QC", True)
    assert actual.failure_type == expected.failure_type
    assert actual.failed_post_data_gen == expected.failed_post_data_gen
