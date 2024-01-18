from enum import Enum
from datetime import date


# All possible statuses, in order of precidence
# Ex. a cell line with one completed attempt and one failed attempt should be considered complete
class Status(Enum):
    failed = 1
    in_progress = 2
    complete = 3


status_display_name_dict = {
    Status.failed: "Failed",
    Status.in_progress: "In Progress",
    Status.complete: "Data in Portal",
}

screen_substatus_mapping_dict = {
    "N/A": "Screen Fail - Other",
    "Initiation": "Fail Initiation",
    "not started": "Fail Initiation",
    "Pre-screen QC": "Fail Pre-Screen QC",
    "Fail Prep for seq": "Fail Pre-Screen QC",
    "Pooled screen queue": "Fail Pre-Screen QC",
    "Pooled Screen Dev": "Fail Pre-Screen QC",
    "Cas transduction": "Fail Cas transduction",
}


class ScreenFailureDetails:
    def __init__(self, failure_type: str, failed_post_data_gen: bool) -> None:
        self.failure_type = failure_type
        self.failed_post_data_gen = failed_post_data_gen


# Each ModelStatusSummary object contains all of the info we need about the model
class ModelStatusSummary:
    def __init__(self, lineage: str, peddep_subgroup: str = None) -> None:
        self.lineage = lineage
        self.peddep_subgroup = peddep_subgroup
        self.statuses = {}
        self.screen_failure_details = None
        self.screen_failure_count = 0

    def update_status(
        self,
        datatype: str,
        attempt_status: Status,
        failure_details: ScreenFailureDetails = None,
    ) -> None:
        # if we don't have a status yet, use the attempt status
        if self.statuses.get(datatype) is None:
            self.statuses[datatype] = attempt_status
            self.screen_failure_details = failure_details
        # if multiple statuses exist, determine which takes precidence
        elif attempt_status is not None:
            new_status = (
                attempt_status
                if attempt_status.value > self.statuses[datatype].value
                else self.statuses[datatype]
            )
            if new_status != self.statuses[datatype]:
                self.statuses[datatype] = new_status
                self.screen_failure_details = failure_details

        # count crispr screen failures
        if datatype == "crispr" and attempt_status == Status.failed:
            self.screen_failure_count = self.screen_failure_count + 1

    def to_json_dict(self) -> dict:
        json = {
            datatype: status_display_name_dict[status_enum]
            for datatype, status_enum in self.statuses.items()
            if status_enum
        }
        json["lineage"] = self.lineage
        json["peddep_subgroup"] = self.peddep_subgroup
        if self.screen_failure_details:
            json["screen_failure_type"] = self.screen_failure_details.failure_type
            json[
                "screen_failed_post_data_gen"
            ] = self.screen_failure_details.failed_post_data_gen
            json["screen_failure_count"] = self.screen_failure_count
        return json


def init_status_dict(cursor, peddep_only: bool = False):
    peddep_filter = "WHERE model.peddep_line = True" if peddep_only else ""
    cursor.execute(
        """
        SELECT model.model_id, depmap_model_type.lineage, model.peddep_subgroup FROM model
        LEFT JOIN depmap_model_type on depmap_model_type.depmap_code = model.depmap_model_type {};""".format(
            peddep_filter
        )
    )
    status_dict = {}
    for model_id, lineage, peddep_subgroup in cursor.fetchall():
        status_dict[model_id] = ModelStatusSummary(lineage, peddep_subgroup)
    return status_dict


def add_omics_statuses(cursor, status_dict):
    for datatype in ["wgs", "rna"]:
        cursor.execute(
            f"""
            SELECT mc.model_id, status, main_sequencing_id, blacklist_omics, consortium_release_date FROM omics_profile
            JOIN model_condition AS mc ON model_condition = mc.model_condition_id
            WHERE omics_profile.datatype='{datatype}';"""
        )
        for (
            model_id,
            status,
            main_sequencing_id,
            blacklist,
            consortium_release_date,
        ) in cursor.fetchall():
            if status_dict.get(model_id):
                status_dict[model_id].update_status(
                    datatype=datatype,
                    attempt_status=get_omics_status(
                        status, main_sequencing_id, blacklist, consortium_release_date
                    ),
                )
    return status_dict


def add_crispr_statuses(cursor, status_dict):
    cursor.execute(
        """
        SELECT mc.model_id, status, substatus, screener_qc_pass, cdsqc, blacklist, consortium_release_date
        FROM screen
        JOIN model_condition AS mc ON screen.model_condition_id = mc.model_condition_id
        WHERE library IN ('Avana','Humagne-CD');"""
    )
    for (
        model_id,
        screener_status,
        substatus,
        screener_qc,
        cds_qc,
        blacklist,
        consortium_release_date,
    ) in cursor.fetchall():
        if status_dict.get(model_id) and not blacklist:
            screen_status = get_screen_status(
                screener_status, screener_qc, cds_qc, consortium_release_date
            )
            if screen_status != Status.failed:
                status_dict[model_id].update_status(
                    datatype="crispr", attempt_status=screen_status
                )
            else:
                failure_details = get_screen_failure_details(
                    screener_qc, cds_qc, screener_status, substatus
                )
                status_dict[model_id].update_status(
                    datatype="crispr",
                    attempt_status=screen_status,
                    failure_details=failure_details,
                )
    return status_dict


def get_omics_status(
    profile_status, main_sequencing_id, blacklist, consortium_release_date
) -> Status:
    is_released = (consortium_release_date is not None) and (
        consortium_release_date < date.today()
    )
    if blacklist:
        return Status.failed
    elif main_sequencing_id is not None and is_released:
        return Status.complete
    elif profile_status is not None and "Abandoned" in profile_status:
        return Status.failed
    elif profile_status is not None:
        return Status.in_progress
    else:
        return None


def get_screen_status(status, screener_qc, cds_qc, consortium_release_date) -> Status:
    is_released = (consortium_release_date is not None) and (
        consortium_release_date < date.today()
    )
    if screener_qc == "PASS" and cds_qc == "PASS" and is_released:
        return Status.complete
    if (
        status == "Terminal Fail"
        or cds_qc_failed(cds_qc)
        or screener_qc_failed(screener_qc)
    ):
        return Status.failed
    elif screener_qc == "PASS" or (status is not None):
        return Status.in_progress
    else:
        return None


# Identify the category and timing of a failure for a given screen
# returns failure type label and failure timing label
def get_screen_failure_details(
    screener_qc, cds_qc, screener_status, substatus
) -> ScreenFailureDetails:
    # Case 1. For terminally failed screens with a defined substatus
    # ----> failure_type is the substatus, failed during screening
    if (screener_status == "Terminal Fail") and (substatus is not None):
        cleaned_substatus = substatus[5:]  # Chop off numeric code at the beginning
        renamed_substatus = screen_substatus_mapping_dict.get(
            cleaned_substatus, cleaned_substatus
        )
        return ScreenFailureDetails(renamed_substatus, False)
    # Case 2. Where the screening status is 'Done' but the screener qc failed:
    # ---> determine the failure type using the QC error message
    if (screener_status == "Done") and screener_qc_failed(screener_qc):
        if screener_qc == "FAIL - Fingerprinting":
            return ScreenFailureDetails("Fail Fingerprinting", True)
        else:
            return ScreenFailureDetails("Post-Screen Fail - Other", True)
    # Case 3. Where screener qc is fine, but cds QC failed
    # ---> failure_type is data QC
    elif (
        (screener_status == "Done")
        and (screener_qc == "PASS")
        and cds_qc_failed(cds_qc)
    ):
        return ScreenFailureDetails("Fail Data QC", True)
    return ScreenFailureDetails("Other", True)


def screener_qc_failed(screener_qc):
    return (screener_qc is not None) and ("FAIL" in screener_qc)


def cds_qc_failed(cds_qc):
    return (
        (cds_qc is not None) and (cds_qc != "PASS") and (cds_qc != "missing CN data;")
    )
