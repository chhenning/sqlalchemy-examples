# SQLAlchemy by Examples

My collections of sqlalchemy's magic!

I'll be using Python 3.10.0 and also `pydantic` and `Faker`.

All code is formatted with `black`.

# Other sites with examples

[SQLAlchemy Examples](https://github.com/sqlalchemy/sqlalchemy/tree/main/examples)

[SQLAlchemy's Wiki](https://github.com/sqlalchemy/sqlalchemy/wiki)

[pygotham 2019](https://github.com/f0rk/pygotham-2019/tree/master/code/pygotham_2019)

[Advanced SQLAlchemy Features You Need To Start Using](https://martinheinz.dev/blog/28)

Beware, some information seem outdated!
[pysheet](https://www.pythonsheets.com/notes/python-sqlalchemy.html)

# Other tools with using SQLAlchemy

[SQLModel](https://github.com/tiangolo/sqlmodel)

[SQLAlchemy Utils](https://github.com/kvesteri/sqlalchemy-utils)

[tabulate](https://pypi.org/project/tabulate/)

[Awesome SQLAlchemy](https://github.com/dahlia/awesome-sqlalchemy)

# SQLAlchemy Notes

## Result

https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Row._fields

https://docs.sqlalchemy.org/en/14/core/connections.html#sqlalchemy.engine.Result

```py
row._mapping
row._fields
```

Cursor results:

- lastrowid -- Get the latest ID from the last row inserted.

- rowcount -- How many rows have been affected by a statement, like `UPDATE` or `DELETE`.

- scalar() -- Fetch the first column of the first row, and close the result set.

- first() -- Fetch the first result or None

## row_number()

One Example

```py
from sqlalchemy import select, func

row_number_stmt = (
    func.row_number().over(order_by=["last", "first"]).label("row_num")
)
```

For descending do:

```py
from sqlalchemy import select, func, desc

row_number_stmt = (
    func.row_number().over(order_by=desc(["last", "first"])).label("row_num")
)
```

# Check of ssl is in use

```py
from sqlalchemy import create_engine
db_string = "postgresql+psycopg2://myuser:******@someserver:5432/somedb"
db = create_engine(db_string)
conn = db.connect()
conn.connection.connection.info.ssl_in_use
```

# Connection Pool

`print(engine.pool.status())`

# Errors

## NotImplementedError: This method is not implemented for SQLAlchemy 2.0.

Don't use an `engine` when executing a query. Use a `connection`.

```py
db = create_engine("sqlite://", echo=False, future=True)

# error
db.execute(select(some_table)).all()

# better
conn = db.connect()
conn.execute(select(some_table)).all()
```

## sqlalchemy.exc.NoSuchModuleError: Can't load plugin: sqlalchemy.dialects:None

Forgot to specify the db dialect when setting up the connection string.

## TypeError: Session.get_bind() got an unexpected keyword argument

Use a dict when passing parameters.

```py
db.execute(
    text("call sp_some_sp(:src, :dst, :dst_desc)"),
    dict(src="LALA", dst="BAL", dst_desc="Here we are."),
)
```
