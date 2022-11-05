#%%
from faker import Faker

from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy import select, insert
from sqlalchemy.orm import declarative_base, sessionmaker
from sqlalchemy.dialects.sqlite import insert as lt_insert

fake = Faker()
Faker.seed(0)  # make sure to also create the same data

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
with engine.begin() as conn:
    Base.metadata.create_all(conn)

Session = sessionmaker(bind=engine, future=True)

with Session() as session:

    names = [dict(first=fake.first_name(), last=fake.last_name()) for _ in range(10)]

    stmt = lt_insert(User).values(names).on_conflict_do_nothing()
    session.execute(stmt)
    session.commit()

    stmt = select(User)
    for row in session.execute(stmt):
        print(row)
