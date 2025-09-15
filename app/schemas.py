from pydantic import BaseModel
from typing import Optional, List, Literal

class ConnectionCreate(BaseModel):
    name: str
    driver: Optional[str] = None
    jdbc_url: Optional[str] = None

class ConnectionOut(BaseModel):
    id: int
    name: str
    driver: Optional[str]
    jdbc_url: Optional[str]
    class Config: from_attributes = True

class ConnectionUpdate(BaseModel):
    name: Optional[str] = None
    driver: Optional[str] = None
    jdbc_url: Optional[str] = None

class WarehouseCreate(BaseModel):
    name: str
    s3_uri: str

class WarehouseOut(BaseModel):
    id: int
    name: str
    s3_uri: str
    class Config: from_attributes = True

class WarehouseUpdate(BaseModel):
    name: Optional[str] = None
    s3_uri: Optional[str] = None

class PolicyCreate(BaseModel):
    name: str
    retention_value: str
    rules_json: Optional[str] = None

class PolicyOut(BaseModel):
    id: int
    name: str
    retention_value: str
    rules_json: Optional[str]
    class Config: from_attributes = True

class PolicyUpdate(BaseModel):
    name: Optional[str] = None
    retention_value: Optional[str] = None
    rules_json: Optional[str] = None

class SourceCreate(BaseModel):
    name: str
    env: str = "dev"
    connection_id: int
    warehouse_id: int
    default_policy_id: int
    legal_hold_default: bool = False

class SourceOut(BaseModel):
    id: int
    name: str
    env: str
    connection_id: int
    warehouse_id: int
    default_policy_id: int
    legal_hold_default: bool
    class Config: from_attributes = True

class SourceUpdate(BaseModel):
    name: Optional[str] = None
    env: Optional[str] = None
    connection_id: Optional[int] = None
    warehouse_id: Optional[int] = None
    default_policy_id: Optional[int] = None
    legal_hold_default: Optional[bool] = None

RuleType = Literal["include","exclude","override_policy","override_hold"]

class RuleCreate(BaseModel):
    type: RuleType
    schema: str
    table: Optional[str] = None
    policy_id: Optional[int] = None
    legal_hold: Optional[bool] = None

class RuleOut(BaseModel):
    id: int
    source_id: int
    type: RuleType
    schema: str
    table: Optional[str]
    policy_id: Optional[int]
    legal_hold: Optional[bool]
    class Config: from_attributes = True

class RuleUpdate(BaseModel):
    type: Optional[RuleType] = None
    schema: Optional[str] = None
    table: Optional[str] = None
    policy_id: Optional[int] = None
    legal_hold: Optional[bool] = None

    
