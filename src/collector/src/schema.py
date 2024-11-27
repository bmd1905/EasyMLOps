from pydantic import BaseModel


class TopLevelSchema(BaseModel):
    event_type: str
    schema_version: str
    payload: dict
