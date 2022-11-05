from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine, MetaData
from sqlalchemy import select
from sqlalchemy.orm import declarative_base, sessionmaker


Base = declarative_base()


class User(Base):
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    first = Column(String)
    last = Column(String)

    def __repr__(self) -> str:
        d = {k: v for k, v in vars(self).items() if not k.startswith("_")}
        return f"<User {d}>"


engine = create_engine("sqlite://", echo=False, future=True)


def create_tables(engine):

    # drop all tables first
    meta = MetaData()
    meta.reflect(bind=engine)

    for table in reversed(meta.sorted_tables):
        table.drop(bind=engine)

    # now create all tables
    with engine.begin() as conn:
        Base.metadata.create_all(conn)


create_tables(engine)
