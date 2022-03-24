# select

Use `scalars()` to retrieve orm objects.

```py
session.scalars(
    select(User)
    .where(User.first == 'Katrin')
    .order_by(User.age.desc())
)
```
