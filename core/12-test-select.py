from sqlalchemy import create_engine, text

db = create_engine("sqlite://", echo=False, future=True)

conn = db.connect()

stmt = text(
    """
    with foo(ID, Name) as (
        select 1, 'Jean Luc Picard' union all
        select 2, 'Benjamin Sisko'
    )
    select * from foo
"""
)

print(*conn.execute(stmt).all())
