from pydantic import BaseModel, model_validator
from typing import List, ClassVar

class ValidateCommand(BaseModel):
    columns: List[str]

    _schema: ClassVar[dict] = {}

    @classmethod
    def set_schema(cls, schema: dict):
        cls._schema = schema

    @model_validator(mode = 'after')
    def validate_columns(self):
        invalid = [col for col in self.columns if col not in self._schema]
        if invalid:
            raise ValueError(f"Columns in the command are not present in the table, {invalid}")
        return self
