from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy import create_engine, select, delete, func
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQLite3Connection


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


Base = declarative_base()


class User(Base):
    __tablename__ = "user_account"
    id = Column(Integer, primary_key=True)
    name = Column(String(30))
    fullname = Column(String)
    addresses = relationship(
        "Address", back_populates="user", cascade="all, delete-orphan"
    )

    def __repr__(self):
        return f"User(id={self.id!r}, name={self.name!r}, fullname={self.fullname!r})"


class Address(Base):
    __tablename__ = "address"
    id = Column(Integer, primary_key=True)
    email_address = Column(String, nullable=False)

    # remove the ondelete and there will be FOREIGN KEY constraint exceptions;
    user_id = Column(
        Integer, ForeignKey("user_account.id", ondelete="CASCADE"), nullable=False
    )
    user = relationship("User", back_populates="addresses")

    def __repr__(self):
        return f"Address(id={self.id!r}, email_address={self.email_address!r}), user_id={self.user_id}"


engine = create_engine("sqlite://", echo=False)
with engine.begin() as conn:
    Base.metadata.create_all(conn)

Session = sessionmaker(bind=engine, future=True)

with Session(engine) as session:
    spongebob = User(
        name="spongebob",
        fullname="Spongebob Squarepants",
        addresses=[Address(email_address="spongebob@sqlalchemy.org")],
    )
    sandy = User(
        name="sandy",
        fullname="Sandy Cheeks",
        addresses=[
            Address(email_address="sandy@sqlalchemy.org"),
            Address(email_address="sandy@squirrelpower.org"),
        ],
    )
    patrick = User(name="patrick", fullname="Patrick Star")
    session.add_all([spongebob, sandy, patrick])
    session.commit()

session = Session(engine)
stmt = select(User).where(User.name.in_(["spongebob", "sandy"]))
for user in session.scalars(stmt):
    print(user.addresses)

session = Session(engine)
stmt = select(User).where(User.name == "patrick")
patrick = session.scalars(stmt).one()
patrick.addresses.append(Address(email_address="patrickstar@sqlalchemy.org"))
session.commit()

##############
def count():
    session = Session(engine)
    count_stmt = select(func.count()).select_from(User)
    print("num users:", session.execute(count_stmt).scalar())

    count_stmt = select(func.count()).select_from(Address)
    print("num addresses:", session.execute(count_stmt).scalar())


def delete_one():
    session = Session(engine)
    stmt = delete(User).where(User.name == "patrick")
    session.execute(stmt)
    session.commit()


def print_everything():
    session = Session(engine)
    for u in session.scalars(select(User)).all():
        print(u)

    for a in session.scalars(select(Address)).all():
        print(a)


count()
delete_one()
count()
print_everything()
