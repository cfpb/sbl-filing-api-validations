from fastapi import Depends, Request
from regtech_api_commons.api.router_wrapper import Router
from sbl_validation_service.services.validator import validate_and_update_submission
from typing import Annotated

from sbl_validation_service.entities.engine.engine import get_session
from sbl_validation_service.entities.models.dto import ValidationDTO

from sqlalchemy.orm import Session


def set_db(request: Request, session: Annotated[Session, Depends(get_session)]):
    request.state.db_session = session


router = Router(dependencies=[Depends(set_db)])


@router.post("/submission")
def validate_file(request: Request, submission: ValidationDTO):
    validate_and_update_submission(
        request.state.db_session, submission.period, submission.lei, submission.submission_id
    )
