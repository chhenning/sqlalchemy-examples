# the main differene to 01 is that the relationship's cascade is set to: cascade="all, delete-orphan"

from sqlalchemy import Column, ForeignKey, Integer, String
from sqlalchemy import create_engine, select, delete, func
from sqlalchemy.orm import relationship, sessionmaker
from sqlalchemy.ext.declarative import as_declarative, declared_attr

from sqlalchemy import event
from sqlalchemy.engine import Engine
from sqlite3 import Connection as SQLite3Connection

import logging

# logging.basicConfig()
# logging.getLogger("sqlalchemy.engine").setLevel(logging.DEBUG)


@event.listens_for(Engine, "connect")
def _set_sqlite_pragma(dbapi_connection, connection_record):
    if isinstance(dbapi_connection, SQLite3Connection):
        cursor = dbapi_connection.cursor()
        cursor.execute("PRAGMA foreign_keys=ON;")
        cursor.close()


@as_declarative()
class Base:
    @declared_attr
    def __tablename__(cls) -> str:
        return cls.__name__.lower()

    def as_dict(self):
        return {c.name: getattr(self, c.name) for c in self.__table__.columns}


class Plan(Base):
    __tablename__ = "plan"
    id = Column(Integer, primary_key=True)
    name = Column(String)

    features = relationship(
        "Feature", back_populates="plan", cascade="all, delete-orphan"
    )


class Tier(Base):
    __tablename__ = "tier"
    id = Column(Integer, primary_key=True)
    name = Column(String)

    features = relationship(
        "Feature", back_populates="tier", cascade="all, delete-orphan"
    )


class Header(Base):
    __tablename__ = "header"
    id = Column(Integer, primary_key=True)
    name = Column(String)

    features = relationship(
        "Feature", back_populates="header", cascade="all, delete-orphan"
    )


class Feature(Base):
    __tablename__ = "feature"
    id = Column(Integer, primary_key=True)
    plan_id = Column(Integer, ForeignKey("plan.id"))
    tier_id = Column(Integer, ForeignKey("tier.id"))
    header_id = Column(Integer, ForeignKey("header.id"))
    name = Column(String)

    plan = relationship("Plan", back_populates="features", uselist=False)
    tier = relationship("Tier", back_populates="features", uselist=False)
    header = relationship("Header", back_populates="features", uselist=False)


engine = create_engine("sqlite://", echo=False, future=True)
with engine.begin() as conn:
    Base.metadata.create_all(conn)

Session = sessionmaker(bind=engine, future=True)


def create():
    with Session() as db:
        p = Plan(name="Plan 1")
        t = Tier(name="Tier 1")
        h = Header(name="Header 1")
        db.add(p)
        db.add(t)
        db.add(h)
        db.flush()

        f = Feature(name="Feature 1")
        p.features.append(f)
        t.features.append(f)
        h.features.append(f)

        db.flush()
        db.commit()


def print_everything():
    with Session() as db:
        [print(p.as_dict()) for p in db.scalars(select(Plan)).all()]
        [print(t.as_dict()) for t in db.scalars(select(Tier)).all()]
        [print(h.as_dict()) for h in db.scalars(select(Header)).all()]
        [print(f.as_dict()) for f in db.scalars(select(Feature)).all()]


def delete_tier():
    with Session() as db:
        print("----------------")
        print("----------------")
        print("----------------")

        ## This will result in a Foreign Key constraint violation
        db.execute(delete(Feature).where(Feature.tier_id == 1))
        db.execute(delete(Tier).where(Tier.id == 1))
        db.commit()

        # This will delete the feature
        # t = db.scalars(select(Tier).where(Tier.id == 1)).one_or_none()
        # db.delete(t)
        # db.commit()
        print("----------------")
        print("----------------")
        print("----------------")


create()
print_everything()
delete_tier()
print_everything()
