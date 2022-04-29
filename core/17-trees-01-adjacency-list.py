import random
import string

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String
from sqlalchemy import select, func, desc, insert, text


def random_lower_string() -> str:
    return "".join(random.choices(string.ascii_lowercase, k=32))


db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()

node_table = Table(
    "node",
    meta,
    Column("id", Integer),
    Column("parent_id", Integer),
    Column("data", String, nullable=False),
)

meta.create_all(db)


def insert_nodes(num: int = 10):
    nodes = [
        dict(id=1, parent_id=None, data=random_lower_string()),
        # branch
        dict(id=2, parent_id=1, data=random_lower_string()),
        dict(id=3, parent_id=2, data=random_lower_string()),
        # branch
        dict(id=4, parent_id=1, data=random_lower_string()),
        dict(id=5, parent_id=4, data=random_lower_string()),
        dict(id=6, parent_id=4, data=random_lower_string()),
        dict(id=7, parent_id=6, data=random_lower_string()),
    ]

    with db.connect() as conn:
        conn.execute(insert(node_table), nodes)
        conn.commit()


def select_all():
    with db.connect() as conn:
        for row in conn.execute(select(node_table)).all():
            print(row)


if __name__ == "__main__":
    insert_nodes()

    # go up one level
    query = """
    select c1.*, c2.*
    from node c1 left outer join node c2 on c1.parent_id = c2.id
    """

    # go up two levels
    query = """
    select c1.*, c2.*, c3.*
    from node c1
    left outer join node c2 on c1.parent_id = c2.id
    left outer join node c3 on c2.parent_id = c3.id
    """

    # cte
    query = """
    with tree(id, parent_id, data, depth) as
    (
        select *, 0 from node where parent_id is null
        union all
        select c.*, t.depth+1 as depth from tree t
        join node c on t.id = c.parent_id
    )
    select * from tree where id = 4
    """

    with db.connect() as conn:
        for row in conn.execute(text(query)).all():
            print(row)
