# sqlite specific examples

## Foreign Keys

When relying on foreign keys make sure to emit the following statement:

`PRAGMA foreign_keys = ON`

This is best done automatically when connecting to a database, via:

```py
@event.listens_for(Engine, "connect")
def set_sqlite_pragma(dbapi_connection, connection_record):
    cursor = dbapi_connection.cursor()
    cursor.execute("PRAGMA foreign_keys=ON")
    cursor.close()
```
