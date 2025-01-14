from sqlmodel import Field, SQLModel

class PumsTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    agep: int
    sch: int
    schl: int
    hincp: int
    pwgtp: int

class GniTable(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    constant: float
    capita: int
