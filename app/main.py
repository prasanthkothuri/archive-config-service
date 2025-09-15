from fastapi import FastAPI, Depends, HTTPException, Response
from sqlalchemy.orm import Session
from .database import init_db, get_db, Base
from . import models, schemas
# plan build removed; no resolution import

app = FastAPI(
    title="Retention Policy Service",
    description=(
        "Configure connections, warehouses, policies, sources, and rules. "
        "Includes an export endpoint that returns Airflow-friendly JSON."
    ),
    version="0.1.0",
    openapi_tags=[
        {"name": "Connections", "description": "Manage data source connection references."},
        {"name": "Warehouses", "description": "Manage archive warehouse locations (e.g., S3)."},
        {"name": "Policies", "description": "Retention policy definitions referenced by sources and rules."},
        {"name": "Sources", "description": "Configure sources and defaults; export for orchestration."},
        {"name": "Rules", "description": "Include/exclude tables and override policy or legal hold."},
        {"name": "Export", "description": "Export Airflow-friendly JSON for a source."},
    ],
)

@app.on_event("startup")
def startup():
    init_db(Base)

@app.post(
    "/v1/connections",
    response_model=schemas.ConnectionOut,
    tags=["Connections"],
    summary="Create connection",
)
def create_connection(payload: schemas.ConnectionCreate, db: Session = Depends(get_db)):
    if db.query(models.Connection).filter_by(name=payload.name).first():
        raise HTTPException(409, "Connection name already exists")
    obj = models.Connection(**payload.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.get(
    "/v1/connections",
    response_model=list[schemas.ConnectionOut],
    tags=["Connections"],
    summary="List connections",
)
def list_connections(db: Session = Depends(get_db)):
    return db.query(models.Connection).order_by(models.Connection.id).all()

@app.get(
    "/v1/connections/{id}",
    response_model=schemas.ConnectionOut,
    tags=["Connections"],
    summary="Get connection",
)
def get_connection(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Connection, id)
    if not obj: raise HTTPException(404, "Not found")
    return obj

@app.patch(
    "/v1/connections/{id}",
    response_model=schemas.ConnectionOut,
    tags=["Connections"],
    summary="Update connection",
)
def update_connection(id: int, payload: schemas.ConnectionUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.Connection, id)
    if not obj:
        raise HTTPException(404, "Not found")
    data = payload.model_dump(exclude_unset=True)
    if "name" in data and data["name"] != obj.name:
        if db.query(models.Connection).filter_by(name=data["name"]).first():
            raise HTTPException(409, "Connection name already exists")
    for k, v in data.items():
        setattr(obj, k, v)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.delete(
    "/v1/connections/{id}", status_code=204, tags=["Connections"], summary="Delete connection"
)
def delete_connection(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Connection, id)
    if not obj:
        raise HTTPException(404, "Not found")
    in_use = db.query(models.Source).filter_by(connection_id=id).first()
    if in_use:
        raise HTTPException(400, "Connection in use by sources")
    db.delete(obj); db.commit()
    return Response(status_code=204)

@app.post(
    "/v1/warehouses",
    response_model=schemas.WarehouseOut,
    tags=["Warehouses"],
    summary="Create warehouse",
)
def create_warehouse(payload: schemas.WarehouseCreate, db: Session = Depends(get_db)):
    if db.query(models.Warehouse).filter_by(name=payload.name).first():
        raise HTTPException(409, "Warehouse name already exists")
    obj = models.Warehouse(**payload.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.get(
    "/v1/warehouses",
    response_model=list[schemas.WarehouseOut],
    tags=["Warehouses"],
    summary="List warehouses",
)
def list_warehouses(db: Session = Depends(get_db)):
    return db.query(models.Warehouse).order_by(models.Warehouse.id).all()

@app.get(
    "/v1/warehouses/{id}",
    response_model=schemas.WarehouseOut,
    tags=["Warehouses"],
    summary="Get warehouse",
)
def get_warehouse(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Warehouse, id)
    if not obj: raise HTTPException(404, "Not found")
    return obj

@app.patch(
    "/v1/warehouses/{id}",
    response_model=schemas.WarehouseOut,
    tags=["Warehouses"],
    summary="Update warehouse",
)
def update_warehouse(id: int, payload: schemas.WarehouseUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.Warehouse, id)
    if not obj:
        raise HTTPException(404, "Not found")
    data = payload.model_dump(exclude_unset=True)
    if "name" in data and data["name"] != obj.name:
        if db.query(models.Warehouse).filter_by(name=data["name"]).first():
            raise HTTPException(409, "Warehouse name already exists")
    for k, v in data.items():
        setattr(obj, k, v)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.delete(
    "/v1/warehouses/{id}", status_code=204, tags=["Warehouses"], summary="Delete warehouse"
)
def delete_warehouse(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Warehouse, id)
    if not obj:
        raise HTTPException(404, "Not found")
    in_use = db.query(models.Source).filter_by(warehouse_id=id).first()
    if in_use:
        raise HTTPException(400, "Warehouse in use by sources")
    db.delete(obj); db.commit()
    return Response(status_code=204)

@app.post(
    "/v1/policies",
    response_model=schemas.PolicyOut,
    tags=["Policies"],
    summary="Create policy",
)
def create_policy(payload: schemas.PolicyCreate, db: Session = Depends(get_db)):
    if db.query(models.Policy).filter_by(name=payload.name).first():
        raise HTTPException(409, "Policy name already exists")
    obj = models.Policy(**payload.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.get(
    "/v1/policies/{id}",
    response_model=schemas.PolicyOut,
    tags=["Policies"],
    summary="Get policy",
)
def get_policy(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Policy, id)
    if not obj: raise HTTPException(404, "Not found")
    return obj

@app.get(
    "/v1/policies",
    response_model=list[schemas.PolicyOut],
    tags=["Policies"],
    summary="List policies",
)
def list_policies(db: Session = Depends(get_db)):
    return db.query(models.Policy).order_by(models.Policy.id).all()

@app.patch(
    "/v1/policies/{id}",
    response_model=schemas.PolicyOut,
    tags=["Policies"],
    summary="Update policy",
)
def update_policy(id: int, payload: schemas.PolicyUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.Policy, id)
    if not obj:
        raise HTTPException(404, "Not found")
    data = payload.model_dump(exclude_unset=True)
    if "name" in data and data["name"] != obj.name:
        if db.query(models.Policy).filter_by(name=data["name"]).first():
            raise HTTPException(409, "Policy name already exists")
    for k, v in data.items():
        setattr(obj, k, v)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.delete(
    "/v1/policies/{id}", status_code=204, tags=["Policies"], summary="Delete policy"
)
def delete_policy(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Policy, id)
    if not obj:
        raise HTTPException(404, "Not found")
    in_use_source = db.query(models.Source).filter_by(default_policy_id=id).first()
    in_use_rule = db.query(models.Rule).filter_by(policy_id=id).first()
    if in_use_source or in_use_rule:
        raise HTTPException(400, "Policy in use by sources or rules")
    db.delete(obj); db.commit()
    return Response(status_code=204)

@app.post(
    "/v1/sources",
    response_model=schemas.SourceOut,
    tags=["Sources"],
    summary="Create source",
)
def create_source(payload: schemas.SourceCreate, db: Session = Depends(get_db)):
    if db.query(models.Source).filter_by(name=payload.name).first():
        raise HTTPException(409, "Source name already exists")
    for (cls, key) in [(models.Connection, payload.connection_id),
                       (models.Warehouse, payload.warehouse_id),
                       (models.Policy, payload.default_policy_id)]:
        if not db.get(cls, key):
            raise HTTPException(400, f"Invalid reference id: {cls.__name__}={key}")
    obj = models.Source(**payload.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.get(
    "/v1/sources",
    response_model=list[schemas.SourceOut],
    tags=["Sources"],
    summary="List sources",
)
def list_sources(db: Session = Depends(get_db)):
    return db.query(models.Source).order_by(models.Source.id).all()

@app.get(
    "/v1/sources/{source_id}:export",
    tags=["Export"],
    summary="Export source config",
)
def export_source_config(source_id: int, db: Session = Depends(get_db)):
    src = db.get(models.Source, source_id)
    if not src:
        raise HTTPException(404, "Source not found")

    # Build include/exclude schema->tables from table-level rules
    def group_tables(rules):
        grouped: dict[str, list[dict[str, str]]] = {}
        for r in rules:
            if r.table is None:
                continue
            grouped.setdefault(r.schema, []).append({"name": r.table})
        # Sort tables for determinism
        return [
            {"name": schema, "tables": sorted(tables, key=lambda t: t["name"]) }
            for schema, tables in sorted(grouped.items(), key=lambda kv: kv[0])
        ]

    include_rules = [r for r in src.rules if r.type == "include"]
    exclude_rules = [r for r in src.rules if r.type == "exclude"]

    include_block = {"schemas": group_tables(include_rules)}
    exclude_schemas = group_tables(exclude_rules)
    exclude_block = {} if not exclude_schemas else {"schemas": exclude_schemas}

    return {
        "id": src.name,
        "env": src.env,
        "connection": src.connection.name if src.connection else None,
        "warehouse": src.warehouse.name if src.warehouse else None,
        "default_policy": src.default_policy.name if src.default_policy else None,
        "legal_hold_default": bool(src.legal_hold_default),
        "include": include_block,
        "exclude": exclude_block,
    }

@app.get(
    "/v1/sources/{id}",
    response_model=schemas.SourceOut,
    tags=["Sources"],
    summary="Get source",
)
def get_source(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Source, id)
    if not obj: raise HTTPException(404, "Not found")
    return obj

@app.patch(
    "/v1/sources/{id}",
    response_model=schemas.SourceOut,
    tags=["Sources"],
    summary="Update source",
)
def update_source(id: int, payload: schemas.SourceUpdate, db: Session = Depends(get_db)):
    obj = db.get(models.Source, id)
    if not obj:
        raise HTTPException(404, "Not found")
    data = payload.model_dump(exclude_unset=True)
    # name uniqueness
    if "name" in data and data["name"] != obj.name:
        if db.query(models.Source).filter_by(name=data["name"]).first():
            raise HTTPException(409, "Source name already exists")
    # FK validations if provided
    if "connection_id" in data and not db.get(models.Connection, data["connection_id"]):
        raise HTTPException(400, f"Invalid reference id: Connection={data['connection_id']}")
    if "warehouse_id" in data and not db.get(models.Warehouse, data["warehouse_id"]):
        raise HTTPException(400, f"Invalid reference id: Warehouse={data['warehouse_id']}")
    if "default_policy_id" in data and not db.get(models.Policy, data["default_policy_id"]):
        raise HTTPException(400, f"Invalid reference id: Policy={data['default_policy_id']}")
    for k, v in data.items():
        setattr(obj, k, v)
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.delete(
    "/v1/sources/{id}", status_code=204, tags=["Sources"], summary="Delete source"
)
def delete_source(id: int, db: Session = Depends(get_db)):
    obj = db.get(models.Source, id)
    if not obj:
        raise HTTPException(404, "Not found")
    in_use_rules = db.query(models.Rule).filter_by(source_id=id).first()
    if in_use_rules:
        raise HTTPException(400, "Source has rules; delete rules first")
    db.delete(obj); db.commit()
    return Response(status_code=204)

@app.get(
    "/v1/sources/{source_id}/rules",
    response_model=list[schemas.RuleOut],
    tags=["Rules"],
    summary="List rules",
)
def list_rules(source_id: int, db: Session = Depends(get_db)):
    src = db.get(models.Source, source_id)
    if not src:
        raise HTTPException(404, "Source not found")
    rules = db.query(models.Rule).filter_by(source_id=source_id).order_by(models.Rule.id).all()
    return rules

@app.post(
    "/v1/sources/{source_id}/rules",
    response_model=schemas.RuleOut,
    tags=["Rules"],
    summary="Create rule",
)
def add_rule(source_id: int, payload: schemas.RuleCreate, db: Session = Depends(get_db)):
    src = db.get(models.Source, source_id)
    if not src: raise HTTPException(404, "Source not found")
    if payload.type == "override_policy" and not payload.policy_id:
        raise HTTPException(400, "policy_id required for override_policy")
    obj = models.Rule(source_id=source_id, **payload.model_dump())
    db.add(obj); db.commit(); db.refresh(obj)
    return obj

@app.get(
    "/v1/sources/{source_id}/rules/{rule_id}",
    response_model=schemas.RuleOut,
    tags=["Rules"],
    summary="Get rule",
)
def get_rule(source_id: int, rule_id: int, db: Session = Depends(get_db)):
    src = db.get(models.Source, source_id)
    if not src:
        raise HTTPException(404, "Source not found")
    rule = db.get(models.Rule, rule_id)
    if not rule or rule.source_id != source_id:
        raise HTTPException(404, "Rule not found")
    return rule

@app.patch(
    "/v1/sources/{source_id}/rules/{rule_id}",
    response_model=schemas.RuleOut,
    tags=["Rules"],
    summary="Update rule",
)
def update_rule(source_id: int, rule_id: int, payload: schemas.RuleUpdate, db: Session = Depends(get_db)):
    src = db.get(models.Source, source_id)
    if not src:
        raise HTTPException(404, "Source not found")
    rule = db.get(models.Rule, rule_id)
    if not rule or rule.source_id != source_id:
        raise HTTPException(404, "Rule not found")

    data = payload.model_dump(exclude_unset=True)

    # Determine resulting type after update
    new_type = data.get("type", rule.type)

    # Validate and normalize fields based on type
    if new_type == "override_policy":
        # Ensure policy_id present (either incoming or existing)
        policy_id = data.get("policy_id", rule.policy_id)
        if policy_id is None:
            raise HTTPException(400, "policy_id required for override_policy")
        if not db.get(models.Policy, policy_id):
            raise HTTPException(400, f"Invalid reference id: Policy={policy_id}")
        # legal_hold not relevant
        data.setdefault("legal_hold", None)
    elif new_type == "override_hold":
        # Ensure legal_hold present
        legal_hold = data.get("legal_hold", rule.legal_hold)
        if legal_hold is None:
            raise HTTPException(400, "legal_hold required for override_hold")
        # policy_id not relevant
        data.setdefault("policy_id", None)
    else:
        # include/exclude don't carry policy_id or legal_hold
        data["policy_id"] = None
        data["legal_hold"] = None

    for k, v in data.items():
        setattr(rule, k, v)
    db.add(rule); db.commit(); db.refresh(rule)
    return rule

@app.delete(
    "/v1/sources/{source_id}/rules/{rule_id}",
    status_code=204,
    tags=["Rules"],
    summary="Delete rule",
)
def delete_rule(source_id: int, rule_id: int, db: Session = Depends(get_db)):
    src = db.get(models.Source, source_id)
    if not src:
        raise HTTPException(404, "Source not found")
    rule = db.get(models.Rule, rule_id)
    if not rule or rule.source_id != source_id:
        raise HTTPException(404, "Rule not found")
    db.delete(rule); db.commit()
    return Response(status_code=204)

# /v1/plans:build endpoint removed per requirements

@app.get(
    "/v1/sources/{source_id}/policy:effective",
    tags=["Policies"],
    summary="Get effective policy for a table",
)
def effective_policy(
    source_id: int,
    schema: str,
    table: str,
    db: Session = Depends(get_db),
):
    src = db.get(models.Source, source_id)
    if not src:
        raise HTTPException(404, "Source not found")

    # Resolve policy precedence: default -> schema override -> table override
    policy_id = src.default_policy_id

    schema_override = (
        db.query(models.Rule)
        .filter_by(source_id=source_id, type="override_policy", schema=schema, table=None)
        .order_by(models.Rule.id)
        .first()
    )
    if schema_override and schema_override.policy_id:
        policy_id = schema_override.policy_id

    table_override = (
        db.query(models.Rule)
        .filter_by(source_id=source_id, type="override_policy", schema=schema, table=table)
        .order_by(models.Rule.id)
        .first()
    )
    if table_override and table_override.policy_id:
        policy_id = table_override.policy_id

    policy = db.get(models.Policy, policy_id)
    if not policy:
        raise HTTPException(404, "Effective policy not found")

    # Resolve legal hold: default -> schema override -> table override
    legal_hold = bool(src.legal_hold_default)
    schema_hold = (
        db.query(models.Rule)
        .filter_by(source_id=source_id, type="override_hold", schema=schema, table=None)
        .order_by(models.Rule.id)
        .first()
    )
    if schema_hold and schema_hold.legal_hold is not None:
        legal_hold = bool(schema_hold.legal_hold)
    table_hold = (
        db.query(models.Rule)
        .filter_by(source_id=source_id, type="override_hold", schema=schema, table=table)
        .order_by(models.Rule.id)
        .first()
    )
    if table_hold and table_hold.legal_hold is not None:
        legal_hold = bool(table_hold.legal_hold)

    has_rules = bool(policy.rules_json and str(policy.rules_json).strip())

    return {
        "source_id": src.id,
        "source_name": src.name,
        "schema": schema,
        "table": table,
        "scope": (
            "override_table" if table_override and table_override.policy_id == policy.id
            else "override_schema" if schema_override and schema_override.policy_id == policy.id
            else "default"
        ),
        "policy": {
            "id": policy.id,
            "name": policy.name,
            "retention_value": policy.retention_value,
            "has_rules": has_rules,
            "rules_json": policy.rules_json if has_rules else None,
        },
        "legal_hold": legal_hold,
    }
