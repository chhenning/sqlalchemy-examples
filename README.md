# SQLAlchemy by Examples

My collections of sqlalchemy's magic!

I'll be using Python 3.10.0 and also `pydantic` and `Faker`.

All code is formatted with `black`.

# Other sites with examples

[SQLAlchemy Examples](https://github.com/sqlalchemy/sqlalchemy/tree/main/examples)

[SQLAlchemy's Wiki](https://github.com/sqlalchemy/sqlalchemy/wiki)

[pygotham 2019](https://github.com/f0rk/pygotham-2019/tree/master/code/pygotham_2019)

# Other tools with using SQLAlchemy

[SQLModel](https://github.com/tiangolo/sqlmodel)

[SQLAlchemy Utils](https://github.com/kvesteri/sqlalchemy-utils)

[tabulate](https://pypi.org/project/tabulate/)

[Awesome SQLAlchemy](https://github.com/dahlia/awesome-sqlalchemy)

# SQLAlchemy Notes

## Result

Cursor results:

- lastrowid -- Get the latest ID from the last row inserted.

- rowcount -- How many rows have been affected by a statement, like `UPDATE` or `DELETE`.

- scalar() -- Fetch the first column of the first row, and close the result set.

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
