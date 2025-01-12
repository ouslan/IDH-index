from sqlmodel import Field, SQLModel

class pumspr(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    agep: int
    sch: int
    schl: int
    hincp: int
    pwgtp: int
