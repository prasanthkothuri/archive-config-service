from sqlalchemy import Integer, String, Boolean, ForeignKey, DateTime, func, Text
from sqlalchemy.orm import Mapped, mapped_column, relationship
from .database import Base

class Connection(Base):
    __tablename__ = "connections"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    driver: Mapped[str | None] = mapped_column(String(50), nullable=True)
    jdbc_url: Mapped[str | None] = mapped_column(Text, nullable=True)

class Warehouse(Base):
    __tablename__ = "warehouses"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    s3_uri: Mapped[str] = mapped_column(String(512))

class Policy(Base):
    __tablename__ = "policies"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    retention_value: Mapped[str] = mapped_column(String(32))
    rules_json: Mapped[str | None] = mapped_column(Text, nullable=True)

class Source(Base):
    __tablename__ = "sources"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, index=True)
    env: Mapped[str] = mapped_column(String(32), default="dev")

    connection_id: Mapped[int] = mapped_column(ForeignKey("connections.id"))
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))
    default_policy_id: Mapped[int] = mapped_column(ForeignKey("policies.id"))

    legal_hold_default: Mapped[bool] = mapped_column(Boolean, default=False)

    connection = relationship("Connection", backref="sources")
    warehouse = relationship("Warehouse", backref="sources")
    default_policy = relationship("Policy", foreign_keys=[default_policy_id])

class Rule(Base):
    __tablename__ = "rules"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"), index=True)

    type: Mapped[str] = mapped_column(String(32))
    schema: Mapped[str] = mapped_column(String(256))
    table: Mapped[str | None] = mapped_column(String(256), nullable=True)

    policy_id: Mapped[int | None] = mapped_column(ForeignKey("policies.id"), nullable=True)
    legal_hold: Mapped[bool | None] = mapped_column(Boolean, nullable=True)

    source = relationship("Source", backref="rules")
    policy = relationship("Policy")

class Plan(Base):
    __tablename__ = "plans"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    source_id: Mapped[int] = mapped_column(ForeignKey("sources.id"))
    label: Mapped[str | None] = mapped_column(String(200), nullable=True)
    built_at: Mapped["DateTime"] = mapped_column(DateTime(timezone=True), server_default=func.now())

    source = relationship("Source")

class PlanItem(Base):
    __tablename__ = "plan_items"
    id: Mapped[int] = mapped_column(Integer, primary_key=True, index=True)
    plan_id: Mapped[int] = mapped_column(ForeignKey("plans.id"), index=True)
    schema: Mapped[str] = mapped_column(String(256))
    table: Mapped[str] = mapped_column(String(256))
    action: Mapped[str] = mapped_column(String(16))
    policy_id: Mapped[int | None] = mapped_column(ForeignKey("policies.id"), nullable=True)
    legal_hold: Mapped[bool] = mapped_column(Boolean, default=False)
    connection_id: Mapped[int] = mapped_column(ForeignKey("connections.id"))
    warehouse_id: Mapped[int] = mapped_column(ForeignKey("warehouses.id"))

    plan = relationship("Plan", backref="items")
    policy = relationship("Policy")
