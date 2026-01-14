from __future__ import annotations

import os
from typing import Generator

from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    String,
    Text,
    UniqueConstraint,
    create_engine,
    func,
    inspect,
    text,
)
from sqlalchemy.orm import declarative_base, relationship, sessionmaker

from pathlib import Path

DEFAULT_SQLITE_DIR = Path(os.environ.get("PERSISTENT_DATA_DIR", Path("data")))
DEFAULT_DB_URL = f"sqlite:///{DEFAULT_SQLITE_DIR / 'app.db'}"
DATABASE_URL = os.environ.get("DATABASE_URL", DEFAULT_DB_URL)

connect_args = {}
if DATABASE_URL.startswith("sqlite:"):
    connect_args["check_same_thread"] = False
    # Ensure directory exists for local sqlite usage
    if DATABASE_URL.startswith("sqlite:///"):
        db_path = Path(DATABASE_URL.replace("sqlite:///", ""))
        db_path.parent.mkdir(parents=True, exist_ok=True)
else:
    # For Postgres, allow enforcing SSL when not explicitly present in the URL
    if DATABASE_URL.startswith("postgres://") or DATABASE_URL.startswith("postgresql://"):
        if "sslmode=" not in DATABASE_URL:
            connect_args["sslmode"] = "require"

engine = create_engine(
    DATABASE_URL,
    future=True,
    echo=False,
    connect_args=connect_args,
    pool_pre_ping=True,
    pool_recycle=300,
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False, future=True)
Base = declarative_base()


class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True)
    username = Column(String(64), unique=True, nullable=False, index=True)
    password_hash = Column(String(256), nullable=False)
    is_banned = Column(Boolean, nullable=False, default=False, server_default=text("FALSE"))
    is_admin = Column(Boolean, nullable=False, default=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    threads = relationship("Thread", back_populates="user", cascade="all,delete")
    posts = relationship("Post", back_populates="user", cascade="all,delete")
    dens = relationship("Den", back_populates="owner", cascade="all,delete-orphan")
    memberships = relationship("DenMembership", back_populates="user", cascade="all,delete")
    dens = relationship("Den", back_populates="owner", cascade="all,delete-orphan")
    memberships = relationship("DenMembership", back_populates="user", cascade="all,delete")


class Category(Base):
    __tablename__ = "categories"
    __table_args__ = (UniqueConstraint("parent_id", "slug", name="uq_category_parent_slug"),)

    id = Column(Integer, primary_key=True)
    name = Column(String(120), nullable=False)
    slug = Column(String(160), nullable=False)
    desc = Column(Text, nullable=False)
    parent_id = Column(Integer, ForeignKey("categories.id"), nullable=True, index=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    parent = relationship("Category", remote_side=[id], backref="children")
    threads = relationship("Thread", back_populates="category", cascade="all,delete")


class Thread(Base):
    __tablename__ = "threads"
    __table_args__ = (
        UniqueConstraint("category_id", "slug", name="uq_thread_category_slug"),
    )

    id = Column(Integer, primary_key=True)
    category_id = Column(Integer, ForeignKey("categories.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    title = Column(String(200), nullable=False)
    slug = Column(String(200), nullable=False)
    stream_link = Column(String(1024), nullable=False)
    expires_at = Column(DateTime(timezone=True), nullable=True)
    expires_choice = Column(String(32), nullable=True)
    tag = Column(String(64), nullable=False, default="general", server_default="general")
    reply_count = Column(Integer, nullable=False, default=0)
    clicks = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    category = relationship("Category", back_populates="threads")
    user = relationship("User", back_populates="threads")
    posts = relationship("Post", back_populates="thread", cascade="all,delete")


class Post(Base):
    __tablename__ = "posts"

    id = Column(Integer, primary_key=True)
    thread_id = Column(Integer, ForeignKey("threads.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    parent_id = Column(Integer, ForeignKey("posts.id"), nullable=True, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    thread = relationship("Thread", back_populates="posts")
    user = relationship("User", back_populates="posts")
    parent = relationship("Post", remote_side=[id], backref="replies")


def init_db() -> None:
    """Create tables if they do not exist."""
    Base.metadata.create_all(engine)
    ensure_thread_tag_column()
    ensure_user_ban_column()


def ensure_thread_tag_column() -> None:
    """Add the thread.tag column for existing databases without migrations."""
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns("threads")]
    if "tag" in columns:
        return

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE threads ADD COLUMN tag VARCHAR(64) NOT NULL DEFAULT 'general'"))


def ensure_user_ban_column() -> None:
    """Add the users.is_banned column for existing databases without migrations."""
    inspector = inspect(engine)
    columns = [col["name"] for col in inspector.get_columns("users")]
    if "is_banned" in columns:
        return

    with engine.begin() as conn:
        conn.execute(text("ALTER TABLE users ADD COLUMN is_banned BOOLEAN NOT NULL DEFAULT FALSE"))


class Den(Base):
    __tablename__ = "dens"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_dens_slug"),
    )

    id = Column(Integer, primary_key=True)
    name = Column(String(160), nullable=False)
    slug = Column(String(200), nullable=False, index=True)
    owner_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    is_public = Column(Boolean, nullable=False, default=True, server_default=text("TRUE"))
    invite_code = Column(String(64), nullable=True)
    game_id = Column(Integer, nullable=True)
    stream_url = Column(String(2048), nullable=True)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())
    updated_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        onupdate=func.now(),
    )

    owner = relationship("User", back_populates="dens")
    memberships = relationship("DenMembership", back_populates="den", cascade="all,delete-orphan")
    messages = relationship("DenChatMessage", back_populates="den", cascade="all,delete-orphan")


class DenMembership(Base):
    __tablename__ = "den_memberships"
    __table_args__ = (
        UniqueConstraint("den_id", "user_id", name="uq_den_membership"),
    )

    id = Column(Integer, primary_key=True)
    den_id = Column(Integer, ForeignKey("dens.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    role = Column(String(32), nullable=False, default="member", server_default="member")
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    den = relationship("Den", back_populates="memberships")
    user = relationship("User", back_populates="memberships")


class DenChatMessage(Base):
    __tablename__ = "den_chat_messages"

    id = Column(Integer, primary_key=True)
    den_id = Column(Integer, ForeignKey("dens.id"), nullable=False, index=True)
    user_id = Column(Integer, ForeignKey("users.id"), nullable=False, index=True)
    body = Column(Text, nullable=False)
    created_at = Column(DateTime(timezone=True), nullable=False, server_default=func.now())

    den = relationship("Den", back_populates="messages")
    user = relationship("User")


def get_session() -> Generator:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
