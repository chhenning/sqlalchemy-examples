from curses import echo
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
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
with engine.begin() as conn:
    Base.metadata.create_all(conn)

Session = sessionmaker(bind=engine, future=True)

with Session.begin() as session:

    a = User(first="Kathryn", last="Janeway")
    session.add(a)

    session.add_all(
        [
            User(first="Jean-Luc", last="Picard"),
            User(first="Benjamin", last="Sisko"),
        ]
    )

    session.flush()
    session.commit()


with Session.begin() as session:
    if session.execute(select(User.id).where(User.first == "Benjamin")).first():
        print("Benjamin exists")
    else:
        print("Benjamin doesn't exists")

    if session.execute(select(User.id).where(User.first == "Katrin")).first():
        print("Katrin exists")
    else:
        print("Katrin doesn't exists")
