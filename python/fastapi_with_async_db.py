from __future__ import annotations
from asyncio import current_task
import datetime
from typing import Annotated

from pydantic import BaseModel

import uvicorn

from contextlib import asynccontextmanager

from fastapi import FastAPI, Depends

from sqlalchemy import String, Integer, func, select
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column
from sqlalchemy.ext.asyncio import (
    async_sessionmaker,
    async_scoped_session,
    AsyncSession,
    AsyncAttrs,
    create_async_engine,
    AsyncEngine,
)


# ============= model ===================
class Base(AsyncAttrs, DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    create_date: Mapped[datetime.datetime] = mapped_column(server_default=func.now())


class UserBase(BaseModel):
    name: str


# ================ database ===============
URL_DATABASE = "sqlite+aiosqlite:///test.db"


# Import statements...
class DatabaseSessionManager:
    def __init__(self, db_url: str):
        self.URL_DATABASE = db_url
        self.engine: AsyncEngine | None = None
        self.session_maker = None
        self.session = None

    def init_db(self):
        # Database connection parameters...

        # Creating an asynchronous engine
        self.engine = create_async_engine(self.URL_DATABASE)

        # Creating an asynchronous session class
        self.session_maker = async_sessionmaker(
            autocommit=False, autoflush=False, bind=self.engine
        )
        # self.engine = create_async_engine(
        #     database_url, pool_size=100, max_overflow=0, pool_pre_ping=False
        # )

        # Creating a scoped session
        self.session = async_scoped_session(self.session_maker, scopefunc=current_task)

    async def create_tables(self):
        async with self.engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)

    async def close(self):
        # Closing the database session...
        if self.engine is None:
            raise Exception("DatabaseSessionManager is not initialized")
        await self.engine.dispose()


# Initialize the DatabaseSessionManager
sessionmanager = DatabaseSessionManager(URL_DATABASE)


@asynccontextmanager
async def lifespan(app: FastAPI):
    sessionmanager.init_db()
    sessionmanager.create_tables()
    yield
    await sessionmanager.close()


app = FastAPI(lifespan=lifespan)

async def get_db():
    session = sessionmanager.session()
    if session is None:
        raise Exception("DatabaseSessionManager is not initialized")
    try:
        # Setting the search path and yielding the session...
        yield session
    except Exception:
        await session.rollback()
        raise
    finally:
        # Closing the session after use...
        await session.close()


db_dependency = Annotated[AsyncSession, Depends(get_db)]


@app.post("/add_user")
async def add_user(user: UserBase, db: db_dependency):
    db_user = User(**user.dict())
    db.add(db_user)
    await db.commit()
    await db.refresh(db_user)
    return db_user


@app.get("/users")
async def get_users(db: db_dependency):
    users = (await db.scalars(select(User))).all()
    return {"users": users}


@app.get("/user")
async def get_user(id: int, db: db_dependency):
    user = await db.scalar(select(User).where(User.id == id))
    return user


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
