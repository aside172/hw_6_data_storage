CREATE SCHEMA IF NOT EXISTS memory.dds;

CREATE TABLE IF NOT EXISTS memory.dds.hub_order (
    hk_order      VARCHAR,
    bk_order      BIGINT,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_customer (
    hk_customer   VARCHAR,
    bk_customer   BIGINT,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_part (
    hk_part       VARCHAR,
    bk_part       BIGINT,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_supplier (
    hk_supplier   VARCHAR,
    bk_supplier   BIGINT,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_nation (
    hk_nation     VARCHAR,
    bk_nation     BIGINT,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.hub_region (
    hk_region     VARCHAR,
    bk_region     BIGINT,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.lnk_customer_order (
    hk_lnk_co     VARCHAR,
    hk_customer   VARCHAR,
    hk_order      VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.lnk_nation_region (
    hk_lnk_nr     VARCHAR,
    hk_nation     VARCHAR,
    hk_region     VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.lnk_order_part_supplier (
    hk_lnk_ops    VARCHAR,
    hk_order      VARCHAR,
    hk_part       VARCHAR,
    hk_supplier   VARCHAR,
    line_number   INTEGER,
    load_dts      TIMESTAMP,
    record_source VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_order (
    hk_order        VARCHAR,
    hashdiff        VARCHAR,
    load_dts        TIMESTAMP,
    record_source   VARCHAR,
    order_status    VARCHAR,
    total_price     DOUBLE,
    order_date      DATE,
    order_priority  VARCHAR,
    clerk           VARCHAR,
    ship_priority   INTEGER,
    comment         VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_customer (
    hk_customer   VARCHAR,
    hashdiff      VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR,
    name          VARCHAR,
    address       VARCHAR,
    phone         VARCHAR,
    acctbal       DOUBLE,
    mktsegment    VARCHAR,
    comment       VARCHAR,
    nation_key    BIGINT
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_supplier (
    hk_supplier   VARCHAR,
    hashdiff      VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR,
    name          VARCHAR,
    address       VARCHAR,
    phone         VARCHAR,
    acctbal       DOUBLE,
    comment       VARCHAR,
    nation_key    BIGINT
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_part (
    hk_part       VARCHAR,
    hashdiff      VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR,
    name          VARCHAR,
    mfgr          VARCHAR,
    brand         VARCHAR,
    type          VARCHAR,
    size          INTEGER,
    container     VARCHAR,
    retail_price  DOUBLE,
    comment       VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_order_part_supplier (
    hk_lnk_ops      VARCHAR,
    hashdiff        VARCHAR,
    load_dts        TIMESTAMP,
    record_source   VARCHAR,
    quantity        DOUBLE,
    extended_price  DOUBLE,
    discount        DOUBLE,
    tax             DOUBLE,
    return_flag     VARCHAR,
    line_status     VARCHAR,
    ship_date       DATE,
    commit_date     DATE,
    receipt_date    DATE,
    ship_instruct   VARCHAR,
    ship_mode       VARCHAR,
    comment         VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_region (
    hk_region     VARCHAR,
    hashdiff      VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR,
    r_name        VARCHAR,
    r_comment     VARCHAR
);

CREATE TABLE IF NOT EXISTS memory.dds.sat_nation (
    hk_nation     VARCHAR,
    hashdiff      VARCHAR,
    load_dts      TIMESTAMP,
    record_source VARCHAR,
    n_name        VARCHAR,
    n_comment     VARCHAR
);