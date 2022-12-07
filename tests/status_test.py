import gumbo_client.status as status
import pandas as pd


def test_get_crispr_status():
    # Complete
    assert status.get_crispr_status(status=None, screener_qc="PASS", cds_qc="PASS") == status.Status.complete 
    assert status.get_crispr_status(status="Done", screener_qc="PASS", cds_qc="PASS") == status.Status.complete 
    # Failed
    assert status.get_crispr_status(status="Terminal Fail", screener_qc=None, cds_qc=None) == status.Status.failed 
    assert status.get_crispr_status(status=None, screener_qc="PASS", cds_qc="something went wrong") == status.Status.failed 
    assert status.get_crispr_status(status=None, screener_qc="FAIL", cds_qc=None) == status.Status.failed 
    # In Progress
    assert status.get_crispr_status(status=None, screener_qc="PASS", cds_qc=None) == status.Status.in_progress 
    assert status.get_crispr_status(status="In Progress", screener_qc=None, cds_qc=None) == status.Status.in_progress
    assert status.get_crispr_status(status=None, screener_qc="PASS", cds_qc="missing CN data;") == status.Status.in_progress 
    # No status
    assert status.get_crispr_status(status=None, screener_qc=None, cds_qc=None) == None

