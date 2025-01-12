from sqlmodel import Field, Session, SQLModel


class pumspr(SQLModel, table=True):
    id: int = Field(primary_key=True)
    year: int
    agep: int
    sch: int
    schl: int
    pwgtp: int

