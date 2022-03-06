import pandas as pd

from sqlalchemy import create_engine, text

db = create_engine("sqlite://", echo=False, future=True)
conn = db.connect()

df = pd.DataFrame(
    data=[[0, "10/11/12"], [1, "12/11/10"]], columns=["int_column", "date_column"]
)

df.to_sql("test_data", conn)


stmt = text(
    """
    with categories(ID, Category, Total) as (
        select 1, 'Children Bicycles', 100 union all
        select 2, 'Comfort Bicycles', 76 union all
        select 3, 'Cruisers Bicycles', 99 union all
        select 4, 'Cyclocross Bicycles', 12 union all
        select 5, 'Electric Bikes', 34 union all
        select 6, 'Mountain Bikes', 55 union all
        select 7, 'Road Bikes', 63
    )
    select Category, Total from categories
"""
)

df = pd.read_sql(stmt, conn)
df.reset_index()
print(df.T)
# print(df.pivot(columns="Category", values="Total"))


# df_pivot = df.pivot(columns="asOfDate", values="total")
