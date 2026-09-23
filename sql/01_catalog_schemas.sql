-- P&C Insurance Medallion Architecture: Catalog & Schemas
-- ============================================

CREATE CATALOG IF NOT EXISTS pc_insurance;
USE CATALOG pc_insurance;

CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS reference;
CREATE SCHEMA IF NOT EXISTS dq;
