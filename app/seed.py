from sqlalchemy.orm import Session
from .database import init_db, SessionLocal, Base
from . import models

def seed():
    init_db(Base)
    db: Session = SessionLocal()
    try:
        conn = db.query(models.Connection).filter_by(name="pg_doc_db_metadata_conn").first()
        if not conn:
            conn = models.Connection(
                name="pg_doc_db_metadata_conn",
                driver="postgres",
                jdbc_url="jdbc:postgresql://host:5432/doc_db_metadata",
            )
            db.add(conn); db.commit(); db.refresh(conn)

        wh = db.query(models.Warehouse).filter_by(name="dev_warehouse").first()
        if not wh:
            wh = models.Warehouse(name="dev_warehouse", s3_uri="s3://natwest-data-archive-vault")
            db.add(wh); db.commit(); db.refresh(wh)

        pol = db.query(models.Policy).filter_by(name="default_6m").first()
        if not pol:
            pol = models.Policy(name="default_6m", retention_value="6m")
            db.add(pol); db.commit(); db.refresh(pol)

        src = db.query(models.Source).filter_by(name="pg_doc_db_metadata").first()
        if not src:
            src = models.Source(
                name="pg_doc_db_metadata",
                env="dev",
                connection_id=conn.id,
                warehouse_id=wh.id,
                default_policy_id=pol.id,
                legal_hold_default=False
            )
            db.add(src); db.commit(); db.refresh(src)

            db.add(models.Rule(source_id=src.id, type="include", schema="doc_sup_owner", table="bank_holidays"))
            db.add(models.Rule(source_id=src.id, type="include", schema="doc_sup_owner", table="feed"))
            db.add(models.Rule(source_id=src.id, type="include", schema="doc_sup_owner", table="feed_batch"))
            db.add(models.Rule(source_id=src.id, type="include", schema="doc_sup_owner", table="feed_dependencies"))
            db.commit()

        print("Seed complete. Source id:", src.id)
    finally:
        db.close()

if __name__ == "__main__":
    seed()
