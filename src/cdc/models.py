from postgresql_client import Base
from sqlalchemy import BigInteger, Column, DateTime, Float, String, func


class Event(Base):
    __tablename__ = "events"

    event_time = Column(
        DateTime(timezone=True),
        primary_key=True,
        server_default=func.timezone("UTC", func.current_timestamp()),
    )
    event_type = Column(String(50))
    product_id = Column(BigInteger)
    category_id = Column(BigInteger)
    category_code = Column(String(255))
    brand = Column(String(255))
    price = Column(Float)
    user_id = Column(BigInteger)
    user_session = Column(String(255))
