from __future__ import annotations
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
    AsyncSession,
    AsyncAttrs,
    create_async_engine,
)


URL_DATABASE = "mysql+aiomysql://final:final4188@192.168.168.72/test_case"
engine = create_async_engine(URL_DATABASE)
SessionLocal = async_sessionmaker(engine)


class Base(AsyncAttrs, DeclarativeBase):
    pass


class User(Base):
    __tablename__ = "users"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(256), nullable=False)
    create_date: Mapped[datetime.datetime] = mapped_column(server_default=func.now())


class UserBase(BaseModel):
    name: str


@asynccontextmanager
async def lifespan(app: FastAPI):
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
    yield


app = FastAPI(lifespan=lifespan)


async def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        await db.close()


db_dependency = Annotated[AsyncSession, Depends(get_db)]


@app.post("/add_user")
async def add_user(user: UserBase, db: db_dependency):
    db_user = User(name=user.name)
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
    user = await db.scalar_one(select(User).where(User.id == id))
    return user


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8001)
