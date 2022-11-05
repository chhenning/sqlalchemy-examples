# Reflect (autoload) from an in-memory database.

from faker import Faker

from sqlalchemy import create_engine, text, MetaData, Table, insert, select

fake = Faker()
Faker.seed(0)  # make sure to also create the same data


db = create_engine("sqlite://", echo=False, future=True)

with db.connect() as conn:
    conn.execute(
        text(
            "create table test_tbl (id integer primary key, first text not null, last text not null)"
        )
    )


# create a list of dicts
names = [dict(first=fake.first_name(), last=fake.last_name()) for _ in range(10)]

with db.connect() as conn:
    metadata_obj = MetaData()
    test_tbl = Table("test_tbl", metadata_obj, autoload_with=db)

    conn.execute(insert(test_tbl, names))
    conn.commit()

with db.connect() as conn:
    metadata_obj = MetaData()
    test_tbl = Table("test_tbl", metadata_obj, autoload_with=db)

    for row in conn.execute(select(test_tbl)).all():
        print(row)
