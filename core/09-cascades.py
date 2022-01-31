# Standalone example

# A simple example showing the cascades in action.

from faker import Faker

from sqlalchemy import create_engine, MetaData, event
from sqlalchemy import Table, Column, Integer, String, ForeignKey
from sqlalchemy import select, update, func, delete

from sqlalchemy.engine import Engine


# to make foreign keys work we need to instruct sqlite to do so.
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


fake = Faker()
Faker.seed(0)  # make sure to also create the same data


db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

# taken from https://www.sqlite.org/foreignkeys.html
artist = Table(
    "artist",
    meta,
    Column("id", Integer, primary_key=True),
    Column("name", String),
)

track = Table(
    "track",
    meta,
    Column("id", Integer, primary_key=True),
    Column(
        "artist_id",
        ForeignKey("artist.id", onupdate="CASCADE", ondelete="CASCADE"),
        nullable=False,
    ),
    Column("name", String),
)


def create_db():
    """
    Create the table. Aka, DDL.
    """
    meta.create_all(db)


def insert_data():
    with db.connect() as conn:
        id = conn.execute(artist.insert(), dict(name="Dean Martin")).lastrowid
        conn.execute(track.insert(), dict(artist_id=id, name="That's Amore"))
        conn.execute(track.insert(), dict(artist_id=id, name="Christmas Blues"))

        id = conn.execute(artist.insert(), dict(name="Frank Sinatra")).lastrowid
        conn.execute(track.insert(), dict(artist_id=id, name="My Way"))

        conn.commit()


def print_data():
    stmt = (
        select(artist, track)
        .join_from(artist, track, isouter=True)
        .order_by(artist.c.name)
    )

    with db.connect() as conn:
        for row in conn.execute(stmt).all():
            print(row)


def update_data():
    """
    Change the id of one of the artist. The cascade would update the corresponding artist's ids
    in the track table.
    """
    New_Frank_Sinatra_ID = 100
    with db.connect() as conn:
        stmt = (
            update(artist)
            .where(artist.c.name == "Dean Martin")
            .values(id=New_Frank_Sinatra_ID)
        )
        conn.execute(stmt)
        conn.commit()

    with db.connect() as conn:
        num_ids = conn.execute(
            select(func.count(track.c.id)).where(
                track.c.artist_id == New_Frank_Sinatra_ID
            )
        ).scalar()
        assert num_ids == 2


def delete_data():
    """
    Delete an artist and make sure all tracks are deleted as well.
    """
    TO_BE_DELETED_ARTIST = "Frank Sinatra"
    artist_id = (
        db.connect()
        .execute(select(artist.c.id).where(artist.c.name == TO_BE_DELETED_ARTIST))
        .scalar()
    )

    with db.connect() as conn:
        stmt = delete(artist).where(artist.c.id == artist_id)
        conn.execute(stmt)
        conn.commit()

    # verify
    with db.connect() as conn:
        stmt = select(func.count(artist.c.id)).where(artist.c.id == artist_id)
        assert conn.execute(stmt).scalar() == 0

        stmt = select(func.count(track.c.id)).where(track.c.artist_id == artist_id)
        assert conn.execute(stmt).scalar() == 0


def run():
    create_db()
    insert_data()

    update_data()
    delete_data()


if __name__ == "__main__":
    run()
