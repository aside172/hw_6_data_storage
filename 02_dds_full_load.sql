-- =========================================================
-- DDS FULL LOAD (Data Vault) - Trino safe
-- Source: tpch.tiny
-- Hash keys use namespaces: 'ORDER|', 'CUSTOMER|', etc.
-- =========================================================

-- =========================================================
-- 1) HUBS
-- =========================================================

INSERT INTO memory.dds.hub_region
SELECT DISTINCT
    to_hex(md5(to_utf8('REGION|' || CAST(r.regionkey AS varchar)))) AS hk_region,
    r.regionkey                                                    AS bk_region,
    current_timestamp                                              AS load_dts,
    'tpch.tiny.region'                                             AS record_source
FROM tpch.tiny.region r
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.hub_region h
    WHERE h.hk_region = to_hex(md5(to_utf8('REGION|' || CAST(r.regionkey AS varchar))))
);

INSERT INTO memory.dds.hub_nation
SELECT DISTINCT
    to_hex(md5(to_utf8('NATION|' || CAST(n.nationkey AS varchar)))) AS hk_nation,
    n.nationkey                                                    AS bk_nation,
    current_timestamp                                              AS load_dts,
    'tpch.tiny.nation'                                             AS record_source
FROM tpch.tiny.nation n
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.hub_nation h
    WHERE h.hk_nation = to_hex(md5(to_utf8('NATION|' || CAST(n.nationkey AS varchar))))
);

INSERT INTO memory.dds.hub_customer
SELECT DISTINCT
    to_hex(md5(to_utf8('CUSTOMER|' || CAST(c.custkey AS varchar)))) AS hk_customer,
    c.custkey                                                      AS bk_customer,
    current_timestamp                                              AS load_dts,
    'tpch.tiny.customer'                                           AS record_source
FROM tpch.tiny.customer c
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.hub_customer h
    WHERE h.hk_customer = to_hex(md5(to_utf8('CUSTOMER|' || CAST(c.custkey AS varchar))))
);

INSERT INTO memory.dds.hub_supplier
SELECT DISTINCT
    to_hex(md5(to_utf8('SUPPLIER|' || CAST(s.suppkey AS varchar)))) AS hk_supplier,
    s.suppkey                                                      AS bk_supplier,
    current_timestamp                                              AS load_dts,
    'tpch.tiny.supplier'                                           AS record_source
FROM tpch.tiny.supplier s
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.hub_supplier h
    WHERE h.hk_supplier = to_hex(md5(to_utf8('SUPPLIER|' || CAST(s.suppkey AS varchar))))
);

INSERT INTO memory.dds.hub_part
SELECT DISTINCT
    to_hex(md5(to_utf8('PART|' || CAST(p.partkey AS varchar)))) AS hk_part,
    p.partkey                                                   AS bk_part,
    current_timestamp                                           AS load_dts,
    'tpch.tiny.part'                                            AS record_source
FROM tpch.tiny.part p
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.hub_part h
    WHERE h.hk_part = to_hex(md5(to_utf8('PART|' || CAST(p.partkey AS varchar))))
);

INSERT INTO memory.dds.hub_order
SELECT DISTINCT
    to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar)))) AS hk_order,
    o.orderkey                                                   AS bk_order,
    current_timestamp                                            AS load_dts,
    'tpch.tiny.orders'                                           AS record_source
FROM tpch.tiny.orders o
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.hub_order h
    WHERE h.hk_order = to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar))))
);

-- =========================================================
-- 2) LINKS
-- =========================================================

-- Nation ↔ Region
INSERT INTO memory.dds.lnk_nation_region
SELECT DISTINCT
    to_hex(md5(to_utf8(
        'NR|' ||
        CAST(n.nationkey AS varchar) || '|' ||
        CAST(n.regionkey AS varchar)
    ))) AS hk_lnk_nr,
    to_hex(md5(to_utf8('NATION|' || CAST(n.nationkey AS varchar)))) AS hk_nation,
    to_hex(md5(to_utf8('REGION|' || CAST(n.regionkey AS varchar)))) AS hk_region,
    current_timestamp                                              AS load_dts,
    'tpch.tiny.nation'                                             AS record_source
FROM tpch.tiny.nation n
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.lnk_nation_region l
    WHERE l.hk_lnk_nr = to_hex(md5(to_utf8(
        'NR|' ||
        CAST(n.nationkey AS varchar) || '|' ||
        CAST(n.regionkey AS varchar)
    )))
);

-- Customer ↔ Order
INSERT INTO memory.dds.lnk_customer_order
SELECT DISTINCT
    to_hex(md5(to_utf8(
        'CO|' ||
        CAST(o.custkey AS varchar) || '|' ||
        CAST(o.orderkey AS varchar)
    ))) AS hk_lnk_co,
    to_hex(md5(to_utf8('CUSTOMER|' || CAST(o.custkey AS varchar)))) AS hk_customer,
    to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar))))   AS hk_order,
    current_timestamp                                               AS load_dts,
    'tpch.tiny.orders'                                              AS record_source
FROM tpch.tiny.orders o
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.lnk_customer_order l
    WHERE l.hk_lnk_co = to_hex(md5(to_utf8(
        'CO|' ||
        CAST(o.custkey AS varchar) || '|' ||
        CAST(o.orderkey AS varchar)
    )))
);

-- Order ↔ Part ↔ Supplier (transactional link: includes line_number)
INSERT INTO memory.dds.lnk_order_part_supplier
SELECT DISTINCT
    to_hex(md5(to_utf8(
        'OPS|' ||
        CAST(li.orderkey AS varchar) || '|' ||
        CAST(li.partkey AS varchar)  || '|' ||
        CAST(li.suppkey AS varchar)  || '|' ||
        CAST(li.linenumber AS varchar)
    ))) AS hk_lnk_ops,
    to_hex(md5(to_utf8('ORDER|' || CAST(li.orderkey AS varchar))))     AS hk_order,
    to_hex(md5(to_utf8('PART|' || CAST(li.partkey AS varchar))))       AS hk_part,
    to_hex(md5(to_utf8('SUPPLIER|' || CAST(li.suppkey AS varchar))))   AS hk_supplier,
    li.linenumber                                                      AS line_number,
    current_timestamp                                                  AS load_dts,
    'tpch.tiny.lineitem'                                               AS record_source
FROM tpch.tiny.lineitem li
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.lnk_order_part_supplier l
    WHERE l.hk_lnk_ops = to_hex(md5(to_utf8(
        'OPS|' ||
        CAST(li.orderkey AS varchar) || '|' ||
        CAST(li.partkey AS varchar)  || '|' ||
        CAST(li.suppkey AS varchar)  || '|' ||
        CAST(li.linenumber AS varchar)
    )))
);

-- =========================================================
-- 3) SATELLITES ON HUBS (dedupe by hk + hashdiff)
-- =========================================================

-- SAT_REGION
INSERT INTO memory.dds.sat_region
SELECT
    to_hex(md5(to_utf8('REGION|' || CAST(r.regionkey AS varchar)))) AS hk_region,
    to_hex(md5(to_utf8(
        coalesce(r.name,'') || '|' || coalesce(r.comment,'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.region' AS record_source,
    r.name AS r_name,
    r.comment AS r_comment
FROM tpch.tiny.region r
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_region s
    WHERE s.hk_region = to_hex(md5(to_utf8('REGION|' || CAST(r.regionkey AS varchar))))
      AND s.hashdiff  = to_hex(md5(to_utf8(coalesce(r.name,'') || '|' || coalesce(r.comment,''))))
);

-- SAT_NATION
INSERT INTO memory.dds.sat_nation
SELECT
    to_hex(md5(to_utf8('NATION|' || CAST(n.nationkey AS varchar)))) AS hk_nation,
    to_hex(md5(to_utf8(
        coalesce(n.name,'') || '|' || coalesce(n.comment,'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.nation' AS record_source,
    n.name AS n_name,
    n.comment AS n_comment
FROM tpch.tiny.nation n
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_nation s
    WHERE s.hk_nation = to_hex(md5(to_utf8('NATION|' || CAST(n.nationkey AS varchar))))
      AND s.hashdiff  = to_hex(md5(to_utf8(coalesce(n.name,'') || '|' || coalesce(n.comment,''))))
);

-- SAT_CUSTOMER (nation_key kept as attribute)
INSERT INTO memory.dds.sat_customer
SELECT
    to_hex(md5(to_utf8('CUSTOMER|' || CAST(c.custkey AS varchar)))) AS hk_customer,
    to_hex(md5(to_utf8(
        coalesce(c.name,'') || '|' ||
        coalesce(c.address,'') || '|' ||
        coalesce(c.phone,'') || '|' ||
        coalesce(CAST(c.acctbal AS varchar),'') || '|' ||
        coalesce(c.mktsegment,'') || '|' ||
        coalesce(c.comment,'') || '|' ||
        coalesce(CAST(c.nationkey AS varchar),'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.customer' AS record_source,
    c.name AS c_name,
    c.address AS c_address,
    c.phone AS c_phone,
    c.acctbal AS c_acctbal,
    c.mktsegment AS c_mktsegment,
    c.comment AS c_comment,
    c.nationkey AS nation_key
FROM tpch.tiny.customer c
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_customer s
    WHERE s.hk_customer = to_hex(md5(to_utf8('CUSTOMER|' || CAST(c.custkey AS varchar))))
      AND s.hashdiff   = to_hex(md5(to_utf8(
        coalesce(c.name,'') || '|' ||
        coalesce(c.address,'') || '|' ||
        coalesce(c.phone,'') || '|' ||
        coalesce(CAST(c.acctbal AS varchar),'') || '|' ||
        coalesce(c.mktsegment,'') || '|' ||
        coalesce(c.comment,'') || '|' ||
        coalesce(CAST(c.nationkey AS varchar),'')
      )))
);

-- SAT_SUPPLIER (nation_key kept as attribute)
INSERT INTO memory.dds.sat_supplier
SELECT
    to_hex(md5(to_utf8('SUPPLIER|' || CAST(s.suppkey AS varchar)))) AS hk_supplier,
    to_hex(md5(to_utf8(
        coalesce(s.name,'') || '|' ||
        coalesce(s.address,'') || '|' ||
        coalesce(s.phone,'') || '|' ||
        coalesce(CAST(s.acctbal AS varchar),'') || '|' ||
        coalesce(s.comment,'') || '|' ||
        coalesce(CAST(s.nationkey AS varchar),'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.supplier' AS record_source,
    s.name AS s_name,
    s.address AS s_address,
    s.phone AS s_phone,
    s.acctbal AS s_acctbal,
    s.comment AS s_comment,
    s.nationkey AS nation_key
FROM tpch.tiny.supplier s
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_supplier t
    WHERE t.hk_supplier = to_hex(md5(to_utf8('SUPPLIER|' || CAST(s.suppkey AS varchar))))
      AND t.hashdiff   = to_hex(md5(to_utf8(
        coalesce(s.name,'') || '|' ||
        coalesce(s.address,'') || '|' ||
        coalesce(s.phone,'') || '|' ||
        coalesce(CAST(s.acctbal AS varchar),'') || '|' ||
        coalesce(s.comment,'') || '|' ||
        coalesce(CAST(s.nationkey AS varchar),'')
      )))
);

-- SAT_PART
INSERT INTO memory.dds.sat_part
SELECT
    to_hex(md5(to_utf8('PART|' || CAST(p.partkey AS varchar)))) AS hk_part,
    to_hex(md5(to_utf8(
        coalesce(p.name,'') || '|' ||
        coalesce(p.mfgr,'') || '|' ||
        coalesce(p.brand,'') || '|' ||
        coalesce(p.type,'') || '|' ||
        coalesce(CAST(p.size AS varchar),'') || '|' ||
        coalesce(p.container,'') || '|' ||
        coalesce(CAST(p.retailprice AS varchar),'') || '|' ||
        coalesce(p.comment,'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.part' AS record_source,
    p.name AS p_name,
    p.mfgr AS p_mfgr,
    p.brand AS p_brand,
    p.type AS p_type,
    p.size AS p_size,
    p.container AS p_container,
    p.retailprice AS p_retailprice,
    p.comment AS p_comment
FROM tpch.tiny.part p
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_part s
    WHERE s.hk_part = to_hex(md5(to_utf8('PART|' || CAST(p.partkey AS varchar))))
      AND s.hashdiff = to_hex(md5(to_utf8(
        coalesce(p.name,'') || '|' ||
        coalesce(p.mfgr,'') || '|' ||
        coalesce(p.brand,'') || '|' ||
        coalesce(p.type,'') || '|' ||
        coalesce(CAST(p.size AS varchar),'') || '|' ||
        coalesce(p.container,'') || '|' ||
        coalesce(CAST(p.retailprice AS varchar),'') || '|' ||
        coalesce(p.comment,'')
      )))
);

-- SAT_ORDER
INSERT INTO memory.dds.sat_order
SELECT
    to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar)))) AS hk_order,
    to_hex(md5(to_utf8(
        coalesce(o.orderstatus,'') || '|' ||
        coalesce(CAST(o.totalprice AS varchar),'') || '|' ||
        coalesce(CAST(o.orderdate AS varchar),'') || '|' ||
        coalesce(o.orderpriority,'') || '|' ||
        coalesce(o.clerk,'') || '|' ||
        coalesce(CAST(o.shippriority AS varchar),'') || '|' ||
        coalesce(o.comment,'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.orders' AS record_source,
    o.orderstatus AS o_orderstatus,
    o.totalprice AS o_totalprice,
    o.orderdate AS o_orderdate,
    o.orderpriority AS o_orderpriority,
    o.clerk AS o_clerk,
    o.shippriority AS o_shippriority,
    o.comment AS o_comment
FROM tpch.tiny.orders o
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_order s
    WHERE s.hk_order = to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar))))
      AND s.hashdiff = to_hex(md5(to_utf8(
        coalesce(o.orderstatus,'') || '|' ||
        coalesce(CAST(o.totalprice AS varchar),'') || '|' ||
        coalesce(CAST(o.orderdate AS varchar),'') || '|' ||
        coalesce(o.orderpriority,'') || '|' ||
        coalesce(o.clerk,'') || '|' ||
        coalesce(CAST(o.shippriority AS varchar),'') || '|' ||
        coalesce(o.comment,'')
      )))
);

-- =========================================================
-- 4) SATELLITE ON TRANSACTIONAL LINK (lineitem attributes)
-- =========================================================

INSERT INTO memory.dds.sat_order_part_supplier
SELECT
    to_hex(md5(to_utf8(
        'OPS|' ||
        CAST(li.orderkey AS varchar) || '|' ||
        CAST(li.partkey AS varchar)  || '|' ||
        CAST(li.suppkey AS varchar)  || '|' ||
        CAST(li.linenumber AS varchar)
    ))) AS hk_lnk_ops,
    to_hex(md5(to_utf8(
        coalesce(CAST(li.quantity AS varchar),'') || '|' ||
        coalesce(CAST(li.extendedprice AS varchar),'') || '|' ||
        coalesce(CAST(li.discount AS varchar),'') || '|' ||
        coalesce(CAST(li.tax AS varchar),'') || '|' ||
        coalesce(li.returnflag,'') || '|' ||
        coalesce(li.linestatus,'') || '|' ||
        coalesce(CAST(li.shipdate AS varchar),'') || '|' ||
        coalesce(CAST(li.commitdate AS varchar),'') || '|' ||
        coalesce(CAST(li.receiptdate AS varchar),'') || '|' ||
        coalesce(li.shipinstruct,'') || '|' ||
        coalesce(li.shipmode,'') || '|' ||
        coalesce(li.comment,'')
    ))) AS hashdiff,
    current_timestamp AS load_dts,
    'tpch.tiny.lineitem' AS record_source,
    li.quantity AS l_quantity,
    li.extendedprice AS l_extendedprice,
    li.discount AS l_discount,
    li.tax AS l_tax,
    li.returnflag AS l_returnflag,
    li.linestatus AS l_linestatus,
    li.shipdate AS l_shipdate,
    li.commitdate AS l_commitdate,
    li.receiptdate AS l_receiptdate,
    li.shipinstruct AS l_shipinstruct,
    li.shipmode AS l_shipmode,
    li.comment AS l_comment
FROM tpch.tiny.lineitem li
WHERE NOT EXISTS (
    SELECT 1
    FROM memory.dds.sat_order_part_supplier s
    WHERE s.hk_lnk_ops = to_hex(md5(to_utf8(
        'OPS|' ||
        CAST(li.orderkey AS varchar) || '|' ||
        CAST(li.partkey AS varchar)  || '|' ||
        CAST(li.suppkey AS varchar)  || '|' ||
        CAST(li.linenumber AS varchar)
    )))
      AND s.hashdiff = to_hex(md5(to_utf8(
        coalesce(CAST(li.quantity AS varchar),'') || '|' ||
        coalesce(CAST(li.extendedprice AS varchar),'') || '|' ||
        coalesce(CAST(li.discount AS varchar),'') || '|' ||
        coalesce(CAST(li.tax AS varchar),'') || '|' ||
        coalesce(li.returnflag,'') || '|' ||
        coalesce(li.linestatus,'') || '|' ||
        coalesce(CAST(li.shipdate AS varchar),'') || '|' ||
        coalesce(CAST(li.commitdate AS varchar),'') || '|' ||
        coalesce(CAST(li.receiptdate AS varchar),'') || '|' ||
        coalesce(li.shipinstruct,'') || '|' ||
        coalesce(li.shipmode,'') || '|' ||
        coalesce(li.comment,'')
      )))
);
