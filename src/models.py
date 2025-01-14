from sqlmodel import Field, SQLModel
from typing import Optional

class PumsTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    agep: int
    sch: int
    schl: int
    hincp: Optional[int] = Field(default=None)
    pwgtp: int

class GniTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    conts: float
    capita: float
