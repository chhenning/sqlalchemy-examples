from datetime import datetime

from faker import Faker

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, DateTime
from sqlalchemy import delete, select, text, func

fake = Faker()
Faker.seed(0)  # make sure to also create the same data


# create an in-memory table, turn on `echo=True` to see what sql statements
# are emitted by SQLAlchemy.
db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

job_table = Table(
    "job",
    meta,
    Column("id", Integer, primary_key=True),
    Column("priority", Integer, nullable=False, index=True),
    Column("payload", String, nullable=False),
    Column("created_at", DateTime, default=datetime.utcnow),
)


def create_db():
    """
    Create the table. Aka, DDL.
    """
    meta.create_all(db)


def insert_one_row():
    with db.connect() as conn:
        ins_stmt = job_table.insert().values(priority=10, payload="print Hello")
        conn.execute(ins_stmt)
        conn.commit()


def delete_return():
    with db.begin() as conn:

        # sqlalchemy.exc.CompileError: RETURNING is not supported by this dialect's statement compiler.

        # sub_stmt = (
        #     select(job_table.c.id)
        #     .order_by(job_table.c.priority, job_table.c.created_at)
        #     .limit(1)
        #     .subquery()
        # )
        # stmt = delete(job_table).where(job_table.c.id == sub_stmt).returning(job_table)
        # print(stmt)

        query = """
        delete from job where id = (select id from job order by priority, created_at limit 1)
        returning *
        """

        return conn.execute(text(query)).all()


def run():
    create_db()
    insert_one_row()

    print(delete_return())
    assert (
        db.connect().execute(select(func.count()).select_from(job_table)).scalar() == 0
    )


if __name__ == "__main__":
    run()
