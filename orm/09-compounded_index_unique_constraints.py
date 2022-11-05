from sqlalchemy import Column, Integer, String, UniqueConstraint, DateTime, Index
from sqlalchemy.dialects.postgresql import UUID, JSONB
from sqlalchemy.orm import declarative_base

Base = declarative_base()


class Bla(Base):
    __tablename__: str = "yelp_api_business_search"

    id = Column(Integer, primary_key=True)

    foo_id = Column(String, nullable=False, index=True)
    hash = Column(UUID(as_uuid=True), nullable=False, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False)
    response = Column(JSONB, nullable=False)

    __table_args__ = (
        UniqueConstraint("foo_id", "hash", name="unique__bla__foo_id__hash"),
        Index("idx__bla__foo_id__hash", foo_id, hash),
    )
