from curses import echo
from sqlalchemy import Column, Integer, String
from sqlalchemy import create_engine
from sqlalchemy import select
from sqlalchemy.orm import registry, sessionmaker

mapper_registry = registry()


@mapper_registry.mapped
class User:
    __tablename__ = "user"

    id = Column(Integer, primary_key=True)
    first = Column(String)
    last = Column(String)

    def __repr__(self) -> str:
        d = {k: v for k, v in vars(self).items() if not k.startswith("_")}
        return f"<User {d}>"


engine = create_engine("sqlite://", echo=False)
with engine.begin() as conn:
    mapper_registry.metadata.create_all(conn)

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

    ###########
    # find an object

    # before the select stmt the session will do an "autoflush" to commit all changes

    # the select stmt will return one row and ONE column which contains the User object.
    result = session.execute(select(User).filter_by(last="Janeway"))
    print(result.one())

    ###############
    # change an object
    a.last = "New Last Name"

    # an update stmt is issued before the select stmt
    result = session.execute(select(User).filter_by(last="New Last Name"))
    print(result.scalar())

    session.commit()
