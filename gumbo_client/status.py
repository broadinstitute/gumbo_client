from enum import Enum


# All possible statuses, in order of precidence
# Ex. a cell line with one completed attempt and one failed attempt should be considered complete
class Status(Enum):
    failed = 1
    in_progress = 2
    complete = 3


status_display_name_dict = {
    Status.failed: "Failed",
    Status.in_progress: "In Progress",
    Status.complete: "Data in Portal"
}


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
        json = {datatype: status_display_name_dict[status_enum] for datatype, status_enum in self.statuses.items() if status_enum}
        json["lineage"] = self.lineage 
        json["peddep_subgroup"] = self.peddep_subgroup
        json["crispr_failure_type"] = self.crispr_failure_type 
        return json


def init_status_dict(cursor, peddep_only: bool = False):
    peddep_filter = "WHERE model.peddep_line = True" if peddep_only else ""
    cursor.execute("""
        SELECT model.model_id, depmap_model_type.lineage, model.peddep_subgroup FROM model
        LEFT JOIN depmap_model_type on depmap_model_type.depmap_code = model.depmap_model_type {};""".format(peddep_filter))
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
    if blacklist:
        return Status.failed
    elif main_sequencing_id is not None:
        return Status.complete
    elif profile_status is not None and "Abandoned" in profile_status:
        return Status.failed
    elif profile_status is not None:
        return Status.in_progress
    else: 
        return None


def get_crispr_status(status, screener_qc, cds_qc):
    if screener_qc=="PASS" and cds_qc=="PASS":
        return Status.complete
    if status=="Terminal Fail" or cds_qc_failed(cds_qc) or screener_qc_failed(screener_qc):
        return Status.failed
    elif screener_qc=="PASS" or (status is not None):
        return Status.in_progress
    else:
        return None

def get_crispr_failure_type(screener_qc, cds_qc, status, substatus):
    if screener_qc_failed(screener_qc):
        return "Screener QC Failed"
    elif cds_qc_failed(cds_qc):
        return "CDS QC Failed"
    else:
        return status

def screener_qc_failed(screener_qc):
    return (screener_qc is not None) and ("FAIL" in screener_qc)

def cds_qc_failed(cds_qc):
    return (cds_qc is not None) and (cds_qc!="PASS") and (cds_qc!="missing CN data;")
