from pydantic import BaseModel


class ValidationDTO(BaseModel):
    submission_id: int
    lei: str
    period: str
