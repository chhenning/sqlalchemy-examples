# Standalone example

# Same as 01-simple.py but we use pydantic to create dynamic models.
# See: https://pydantic-docs.helpmanual.io/usage/models/#dynamic-model-creation

from typing import Optional

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import select, func, desc

from typing import Optional
from pydantic import BaseModel, create_model

fake = Faker()
Faker.seed(0)  # make sure to also create the same data


# create an in-memory table, turn on `echo=True` to see what sql statements
# are emitted by SQLAlchemy.
db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

person_table = Table(
    "person",
    meta,
    Column("id", Integer, primary_key=True),
    Column("first", String, nullable=False),
    Column("last", String, nullable=False),
)


def create_db():
    """
    Create the table. Aka, DDL.
    """
    meta.create_all(db)


def batch_insert_many_rows(num):
    """
    Batch insert some data into person table.
    """
    # create a list of dicts
    names = [dict(first=fake.first_name(), last=fake.last_name()) for _ in range(num)]

    with db.connect() as conn:
        ins_stmt = person_table.insert()
        conn.execute(ins_stmt, names)
        conn.commit()


def _create_pydantic_model(sqlal_obj):
    """
    Take a SQLAlchemy obj (table or select) and return a pydantic model definition.
    """

    class Config(BaseModel.Config):
        orm_mode = True

    # Optional[str] allows for None values. Otherwise an object creation via
    # M.from_orm(row) might throw an exception.
    types = {"VARCHAR": Optional[str], "INTEGER": int}

    # it really is important to explicitly convert all SQLAlchemy "strings", like col.name.
    d = {str(col.name): (types[str(col.type)], ...) for col in sqlal_obj.columns}
    return create_model(str(sqlal_obj.name), **d, __config__=Config)


def print_data_as_json():

    sel_stmt = person_table.select().order_by("last", "first")
    M = _create_pydantic_model(person_table)

    with db.connect() as conn:
        for row in conn.execute(sel_stmt).all():
            obj = M.from_orm(row)
            print(obj.json())


def run():
    create_db()
    batch_insert_many_rows(10)
    print_data_as_json()


if __name__ == "__main__":
    run()
