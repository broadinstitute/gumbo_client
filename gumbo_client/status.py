from enum import Enum


# All possible statuses, in order of precidence
# Ex. a cell line with one completed attempt and one failed attempt should be considered complete
class Status(Enum):
    available = 1
    failed = 2
    in_progress = 3
    complete = 4


# Each ModelStatusSummary object contains all of the info we need about the model
class ModelStatusSummary:
    def __init__(self, lineage: str, peddep_subgroup: str=None) -> None:
        self.lineage = lineage
        self.peddep_subgroup = peddep_subgroup
        self.statuses = {}
        self.crispr_failure_type = None # used to determine failure type
    
    def update_status(self, datatype: str, attempt_status: str, crispr_failure_type: str=None) -> None:
        # if we don't have a status yet, use the attempt status
        if self.statuses.get(datatype) is None: 
            self.statuses[datatype] = attempt_status
            self.crispr_failure_type = crispr_failure_type
        # if multiple statuses exist, determine which takes precidence 
        elif attempt_status is not None:
            new_status = attempt_status if attempt_status.value > self.statuses[datatype].value else self.statuses[datatype]
            if new_status != self.statuses[datatype]:
                self.statuses[datatype] = new_status
                self.crispr_failure_type = crispr_failure_type

    def to_json_dict(self) -> dict:
        json = {datatype: status_enum.name for datatype, status_enum in self.statuses.items() if status_enum}
        json["lineage"] = self.lineage 
        json["peddep_subgroup"] = self.peddep_subgroup
        json["crispr_failure_type"] = self.crispr_failure_type if self.crispr_failure_type is not None else "Unknown"
        return json


def init_status_dict(cursor, peddep_only: bool = False):
    peddep_filter = "WHERE model.peddep_line = True" if peddep_only else ""
    cursor.execute("""
        SELECT model.model_id, model.lineage, model.peddep_subgroup FROM model {};""".format(peddep_filter))
    status_dict = {}
    for model_id, lineage, peddep_subgroup in cursor.fetchall():
        status_dict[model_id] = ModelStatusSummary(lineage, peddep_subgroup)
    return status_dict


def add_omics_statuses(cursor, status_dict):
    for datatype in ["wgs", "rna"]:
        cursor.execute(f"""
            SELECT mc.model_id, status, main_sequencing_id, blacklist_omics FROM omics_profile
            JOIN model_condition AS mc ON model_condition = mc.model_condition_id
            WHERE omics_profile.datatype='{datatype}';""")
        for model_id, status, main_sequencing_id, blacklist in cursor.fetchall():
            if status_dict.get(model_id):
                status_dict[model_id].update_status(
                    datatype=datatype, 
                    attempt_status=get_omics_status(status, main_sequencing_id, blacklist))
    return status_dict


def add_crispr_statuses(cursor, status_dict):
    cursor.execute("""
        SELECT mc.model_id, status, substatus, screener_qc_pass, cdsqc
        FROM screen
        JOIN model_condition AS mc ON screen.model_condition_id = mc.model_condition_id
        WHERE (destination_datasets is not null and destination_datasets LIKE '%Achilles%');""")
    for model_id, status, substatus, screener_qc, cds_qc in cursor.fetchall():
        if status_dict.get(model_id):
            status_dict[model_id].update_status(
                datatype="crispr", 
                attempt_status=get_crispr_status(status, screener_qc, cds_qc),
                crispr_failure_type=get_crispr_failure_type(screener_qc, cds_qc, status, substatus))
    return status_dict


def get_omics_status(profile_status, main_sequencing_id, blacklist) -> Status:
    # Map from gumbo omics profile records to the 
    status_string_to_enum = {
        "In progress": Status.in_progress,
        "Ordered": Status.in_progress,
        "Done": Status.in_progress,
        " Abandoned": Status.failed
    }
    if blacklist:
        return Status.failed
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

def get_crispr_failure_type(screener_qc, cds_qc, status, substatus):
    screener_qc_failed = (screener_qc is not None) and ("FAIL" in screener_qc)
    cds_qc_failed = (cds_qc is not None) and (cds_qc!="PASS")
    if screener_qc_failed:
        return "Screener QC Failed"
    elif cds_qc_failed:
        return "CDS QC Failed"
    else:
        return status
    pass