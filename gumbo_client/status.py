from enum import Enum


# All possible statuses, in order of precidence
# Ex. a cell line with one completed attempt and one failed attempt should be considered complete
class Status(Enum):
    available = 1
    failed = 2
    in_progress = 3
    complete = 4


# Each ModelConditionStatusSummary object contains all of the info we need about the model condition
class ModelConditionStatusSummary:
    def __init__(self, lineage) -> None:
        self.lineage = lineage
        self.statuses = {}
        self.crispr_substatus = None # used to determine failure type
    
    def update_status(self, datatype: str, attempt_status: str, crispr_substatus: str=None) -> None:
        # if we don't have a status yet, use the attempt status
        if self.statuses.get(datatype) is None: 
            self.statuses[datatype] = attempt_status
            self.crispr_substatus = crispr_substatus
        # if multiple statuses exist, determine which takes precidence 
        elif attempt_status is not None:
            new_status = attempt_status if attempt_status.value > self.statuses[datatype].value else self.statuses[datatype]
            if new_status != self.statuses[datatype]:
                self.statuses[datatype] = new_status
                self.crispr_substatus = crispr_substatus

    def to_json_dict(self) -> dict:
        json = {datatype: status_enum.name for datatype, status_enum in self.statuses.items() if status_enum}
        json["lineage"] = self.lineage 
        json["crispr_substatus"] = self.crispr_substatus if self.crispr_substatus is not None else "Unknown"
        return json


def init_status_dict(cursor, peddep_only: bool = False):
    peddep_filter = "WHERE model.peddep_line = True" if peddep_only else ""
    cursor.execute("""
        SELECT mc.model_condition_id, model.lineage FROM model_condition AS mc
        JOIN model ON mc.model_id = model.model_id {};""".format(peddep_filter))
    status_dict = {}
    for mc_id, lineage in cursor.fetchall():
        status_dict[mc_id] = ModelConditionStatusSummary(lineage)
    return status_dict


def add_omics_statuses(cursor, status_dict):
    datatype_mapping = {"dna": "wgs", "rna": "rna"}
    for return_datatype, db_datatype in datatype_mapping.items():
        cursor.execute(f"""
            SELECT model_condition, status, main_sequencing_id FROM omics_profile  
            WHERE omics_profile.datatype='{db_datatype}' AND model_condition is not null;""")
        for mc_id, status, main_sequencing_id in cursor.fetchall():
            if status_dict.get(mc_id):
                status_dict[mc_id].update_status(
                    datatype=return_datatype, 
                    attempt_status=get_omics_status(status, main_sequencing_id))
    return status_dict


def add_crispr_statuses(cursor, status_dict):
    cursor.execute("""
        SELECT screen.model_condition_id, status, substatus, screener_qc_pass, cdsqc
        FROM screen
        WHERE (destination_datasets is not null and destination_datasets LIKE '%Achilles%');""")
    for mc_id, status, substatus, screener_qc, cds_qc in cursor.fetchall():
        if status_dict.get(mc_id):
            status_dict[mc_id].update_status(
                datatype="crispr", 
                attempt_status=get_crispr_status(status, screener_qc, cds_qc),
                crispr_substatus=substatus)
    return status_dict


def get_omics_status(profile_status, main_sequencing_id) -> Status:
    # Map from gumbo status terms to our enumeration
    status_string_to_enum = {
        "In progress": Status.in_progress,
        "Ordered": Status.in_progress,
        "Done": Status.complete,
        " Abandoned": Status.failed
    }
    if main_sequencing_id is not None:
        return Status.complete
    if profile_status is not None:
        return status_string_to_enum[profile_status]
    else: 
        return None


def get_crispr_status(status, screener_qc, cds_qc):
    if screener_qc=="PASS" and cds_qc=="PASS":
        return Status.complete
    screener_qc_failed = (screener_qc is not None) and ("FAIL" in screener_qc)
    cds_qc_failed = (cds_qc is not None) and (cds_qc!="PASS")
    if status=="Terminal Fail" or status=="Shelved" or screener_qc_failed or cds_qc_failed:
        return Status.failed
    elif status is not None:
        return Status.in_progress
    else:
        return None