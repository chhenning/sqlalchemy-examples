# Use row locking to get a bunch of jobs and delete them once they are processed.

from faker import Faker

from datetime import datetime

from sqlalchemy import Column, Integer, String, DateTime
from sqlalchemy import create_engine, MetaData
from sqlalchemy import func, text, select, insert, update, delete
from sqlalchemy.dialects.postgresql import JSONB, ENUM
from sqlalchemy.engine.url import URL
from sqlalchemy.orm import declarative_base, sessionmaker

fake = Faker()
Faker.seed(0)

#####################

Base = declarative_base()


class Job(Base):
    __tablename__ = "job"

    id = Column(Integer, primary_key=True)
    created_at = Column(DateTime(timezone=True), nullable=False, default=func.now())
    payload = Column(JSONB, nullable=False)


#####################

db_url = {
    "drivername": "postgresql",
    "username": "postgres",
    "password": "foo",
    "host": "localhost",
    "port": 5432,
    "database": "test_queue",
}

engine = create_engine(URL.create(**db_url), future=True, echo=False)
Session = sessionmaker(bind=engine, future=True)

#####################


def create_tables(engine):

    meta = MetaData()
    meta.reflect(bind=engine)

    for table in reversed(meta.sorted_tables):
        table.drop(bind=engine)

    with engine.begin() as conn:
        Base.metadata.create_all(conn)


def insert_jobs(Session):
    jobs = [Job(payload=dict(task=fake.sentence(nb_words=10))) for i in range(10)]

    with Session.begin() as db:
        db.add_all(jobs)

        db.flush()
        db.commit()


def process_jobs(Session) -> None:
    """
    Get a bunch of jobs and process them.
    """

    get_jobs = """
        DELETE FROM
            job
        USING (
            SELECT * FROM job LIMIT 5 FOR UPDATE SKIP LOCKED
        ) q
        WHERE q.id = job.id RETURNING job.*;
    """

    with Session.begin() as db:
        jobs = db.scalars(text(get_jobs)).all()
        print(len(jobs))

        # do something with the jobs

        # now the jobs will be deleted
        db.commit()


def main():
    create_tables(engine=engine)
    insert_jobs(Session=Session)
    process_jobs(Session)


if __name__ == "__main__":
    main()
