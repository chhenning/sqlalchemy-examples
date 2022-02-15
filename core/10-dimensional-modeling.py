# Many data warehouses are used in a fairly formulaic style, known as a star schema (also known as dimensional modeling [55]).""

# From Kleppmann, Martin. Designing Data-Intensive Applications . O'Reilly Media. Kindle Edition.

from random import choice, seed

from sqlalchemy import create_engine, MetaData
from sqlalchemy import Table, Column, Integer, String, ForeignKey, DECIMAL
from sqlalchemy import select, func, insert, text

##############
# sqlite specific stuff
from sqlalchemy import event
from sqlalchemy.engine import Engine


@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()


###############

from faker import Faker

fake = Faker()
Faker.seed(0)  # make sure to also create the same data

seed(0)

db = create_engine("sqlite://", echo=False, future=True)

meta = MetaData()


fact_sale_tbl = Table(
    "fact_sale",
    meta,
    Column("date_key", ForeignKey("dim_date.date_sk"), nullable=False),
    Column("product_key", ForeignKey("dim_product.product_sk"), nullable=False),
    Column("store_key", ForeignKey("dim_store.store_sk"), nullable=False),
    Column("customer_key", ForeignKey("dim_customer.customer_sk"), nullable=False),
    Column("promotion_key", ForeignKey("dim_promotion.promotion_sk"), nullable=False),
    Column("quantity", Integer),
    Column("net_price", DECIMAL),
    Column("discount_price", DECIMAL),
)

dim_product_tbl = Table(
    "dim_product",
    meta,
    Column("product_sk", Integer, primary_key=True),
    Column("sku", String),
    Column("description", String),
    Column("brand", String),
    Column("category", String),
)

dim_store_tbl = Table(
    "dim_store",
    meta,
    Column("store_sk", Integer, primary_key=True),
    Column("state", String),
    Column("city", String),
)


dim_date_tbl = Table(
    "dim_date",
    meta,
    Column("date_sk", Integer, primary_key=True),
    Column("year", Integer),
    Column("month", String),
    Column("day", Integer),
    Column("weekday", String),
)

dim_customer_tbl = Table(
    "dim_customer",
    meta,
    Column("customer_sk", Integer, primary_key=True),
    Column("name", String),
    Column("DOB", String),
)


dim_promotion_tbl = Table(
    "dim_promotion",
    meta,
    Column("promotion_sk", Integer, primary_key=True),
    Column("name", String),
    Column("ad_type", String),
    Column("coupon_type", String),
)


def _fill_dim_date(conn, n):
    conn.execute(
        dim_date_tbl.insert(),
        [
            dict(
                year=fake.year(),
                month=fake.month(),
                day=fake.day_of_month(),
                weekday=fake.day_of_week(),
            )
            for _ in range(n)
        ],
    )
    conn.commit()


def _fill_dim_products(conn, n):
    conn.execute(
        dim_product_tbl.insert(),
        [
            dict(
                sku=fake.bothify(text="??####", letters="ABCDEFGHIJKLMNOPQRSTUVWXYZ"),
                description=fake.bs(),
                brand=fake.company(),
                category=fake.word(),
            )
            for _ in range(n)
        ],
    )
    conn.commit()


def _fill_dim_store(conn, n):
    conn.execute(
        dim_store_tbl.insert(),
        [
            dict(
                state=fake.state(),
                city=fake.city(),
            )
            for _ in range(n)
        ],
    )
    conn.commit()


def _fill_dim_customer(conn, n):
    conn.execute(
        dim_customer_tbl.insert(),
        [
            dict(
                name=fake.name(),
                DOB=fake.date(),
            )
            for _ in range(n)
        ],
    )
    conn.commit()


def _fill_dim_promotion(conn, n):
    conn.execute(
        dim_promotion_tbl.insert(),
        [
            dict(
                name=fake.sentence(),
                ad_type=fake.sentence(
                    nb_words=3,
                    ext_word_list=[
                        "Poster",
                        "Direct Mail",
                        "In-store Sign",
                        "Online Ad",
                    ],
                ),
                coupon_type=fake.sentence(
                    nb_words=3,
                    ext_word_list=[
                        "Manufacturer",
                        "Presale",
                        "Discount",
                        "Reserve",
                    ],
                )
                if choice([True, False])
                else None,
            )
            for _ in range(n)
        ],
    )
    conn.commit()


def create_and_fill(n):
    meta.create_all(db)

    with db.connect() as conn:

        _fill_dim_products(conn, n)
        _fill_dim_store(conn, n)
        _fill_dim_date(conn, n)
        _fill_dim_customer(conn, n)
        _fill_dim_promotion(conn, n)

        conn.commit()
        conn.execute(text("VACUUM"))

        print(*conn.execute(select(dim_promotion_tbl)).all(), sep="\n")


if __name__ == "__main__":
    create_and_fill(10)

# stmt = select(
#     item_table.c.item_id,
#     item_table.c.name,
#     select(property_table.c.name)
#     .where(property_table.c.id == item_table.c.prop_id)
#     .scalar_subquery(),
# ).order_by("name")
