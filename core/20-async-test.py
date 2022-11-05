import asyncio

from sqlalchemy import MetaData, Table, Column, Integer, String, select
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection

# postgresql://postgres:foo@localhost/Dale

meta = MetaData()

t1 = Table("t1", meta, Column("id", Integer, primary_key=True), Column("name", String))


async def async_main():
    engine = create_async_engine(
        "postgresql+asyncpg://postgres:foo@localhost/async_test",
        echo=True,
    )

    async with engine.begin() as conn:
        await conn.run_sync(meta.drop_all)
        await conn.run_sync(meta.create_all)

        await conn.execute(
            t1.insert(), [{"name": "some name 1"}, {"name": "some name 2"}]
        )

    async with engine.connect() as conn:

        # select a Result, which will be delivered with buffered
        # results
        result = await conn.execute(select(t1).where(t1.c.name == "some name 1"))

        print(result.fetchall())

    # for AsyncEngine created in function scope, close and
    # clean-up pooled connections
    await engine.dispose()


asyncio.run(async_main())
