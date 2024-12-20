from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sbl_validation_service.config import settings

engine = create_engine(settings.conn.unicode_string(), echo=True).execution_options(
    schema_translate_map={None: settings.db_schema}
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)


def get_session():
    session = SessionLocal()
    try:
        yield session
    finally:
        session.close()
