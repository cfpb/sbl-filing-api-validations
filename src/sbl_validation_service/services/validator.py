import json
import pandas as pd
import importlib.metadata as imeta
import logging

from io import BytesIO
from fastapi import HTTPException
from regtech_data_validator.create_schemas import validate_phases, ValidationPhase
from regtech_data_validator.data_formatters import df_to_json, df_to_download
from regtech_data_validator.checks import Severity
from sqlalchemy import MetaData, Table, update
from sqlalchemy.orm import Session
from sbl_validation_service.entities.engine.engine import engine
from http import HTTPStatus
from fsspec import AbstractFileSystem, filesystem
from sbl_validation_service.config import settings

log = logging.getLogger(__name__)

REPORT_QUALIFIER = "_report"


def get_submission(period_code: str, lei: str, submission_id: id, extension: str = "csv") -> bytes:
    try:
        fs: AbstractFileSystem = filesystem(**settings.fs_download_config.__dict__)
        file_path = f"{settings.fs_upload_config.root}/upload/{period_code}/{lei}/{submission_id}.{extension}"
        with fs.open(file_path, "rb") as f:
            return f.read()
    except Exception as e:
        log.error(f"Failed to read file {file_path}:", e, exc_info=True, stack_info=True)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="Failed to read file.")


def upload_report(period_code: str, lei: str, file_identifier: str, content: bytes, extension: str = "csv"):
    try:
        fs: AbstractFileSystem = filesystem(settings.fs_upload_config.protocol)
        if settings.fs_upload_config.mkdir:
            fs.mkdirs(f"{settings.fs_upload_config.root}/upload/{period_code}/{lei}", exist_ok=True)
        with fs.open(
            f"{settings.fs_upload_config.root}/upload/{period_code}/{lei}/{file_identifier}.{extension}", "wb"
        ) as f:
            f.write(content)
    except Exception as e:
        log.error("Failed to upload file", e, exc_info=True, stack_info=True)
        raise HTTPException(status_code=HTTPStatus.INTERNAL_SERVER_ERROR, detail="Failed to upload file")


def validate_and_update_submission(session: Session, period_code: str, lei: str, submission_id: int):
    try:
        content = get_submission(period_code, lei, submission_id)
        validator_version = imeta.version("regtech-data-validator")
        updates = {"validation_ruleset_version": validator_version, "state": "VALIDATION_IN_PROGRESS"}
        update_table(session, submission_id, updates)

        df = pd.read_csv(BytesIO(content), dtype=str, na_filter=False)

        # Validate Phases
        result = validate_phases(df, {"lei": lei})

        # Update tables with response
        if not result[0]:
            state = (
                "VALIDATION_WITH_ERRORS"
                if Severity.ERROR.value in result[1]["validation_severity"].values
                else "VALIDATION_WITH_WARNINGS"
            )
        else:
            state = "VALIDATION_SUCCESSFUL"

        validation_json = build_validation_results(result)
        submission_report = df_to_download(result[1])
        upload_report(period_code, lei, str(submission_id) + "_report", submission_report.encode("utf-8"))

        updates = {"validation_json": validation_json, "state": state}
        update_table(session, submission_id, updates)

    except RuntimeError as re:
        log.error("The file is malformed", re, exc_info=True, stack_info=True)
        updates = {"state": "SUBMISSION_UPDATE_MALFORMED"}
        update_table(session, submission_id, updates)

    except Exception as e:
        log.error(
            f"Validation for submission {submission_id} did not complete due to an unexpected error.",
            e,
            exc_info=True,
            stack_info=True,
        )
        updates = {"state": "VALIDATION_ERROR"}
        update_table(session, submission_id, updates)


def build_validation_results(result):
    val_json = json.loads(df_to_json(result[1]))

    if result[2] == ValidationPhase.SYNTACTICAL.value:
        val_res = {"syntax_errors": {"count": len(val_json), "details": val_json}}
    else:
        errors_list = [e for e in val_json if e["validation"]["severity"] == Severity.ERROR.value]
        warnings_list = [w for w in val_json if w["validation"]["severity"] == Severity.WARNING.value]
        val_res = {
            "syntax_errors": {"count": 0, "details": []},
            "logic_errors": {"count": len(errors_list), "details": errors_list},
            "logic_warnings": {"count": len(warnings_list), "details": warnings_list},
        }

    return val_res


def update_table(session: Session, submission_id: id, data: dict):
    table = Table("submission", MetaData(), autoload_with=engine)

    update_stmt = update(table).where(table.c.id == submission_id).values(data)

    session.execute(update_stmt)
    session.commit()
