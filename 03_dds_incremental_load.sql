-- =========================================================
-- DDS INCREMENTAL LOAD (by order date)
-- Param: ${LOAD_DATE}
-- =========================================================

-- =========================================================
-- HUB_ORDER (only orders of the day)
-- =========================================================

INSERT INTO memory.dds.hub_order
SELECT DISTINCT
    to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar)))) AS hk_order,
    o.orderkey                                                   AS bk_order,
    current_timestamp                                            AS load_dts,
    'tpch.tiny.orders'                                           AS record_source
FROM tpch.tiny.orders o
WHERE o.orderdate = DATE '${LOAD_DATE}'
  AND NOT EXISTS (
      SELECT 1
      FROM memory.dds.hub_order h
      WHERE h.hk_order =
            to_hex(md5(to_utf8('ORDER|' || CAST(o.orderkey AS varchar))))
  );

-- =========================================================
-- LINK_CUSTOMER_ORDER
-- =========================================================

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
WHERE o.orderdate = DATE '${LOAD_DATE}'
  AND NOT EXISTS (
      SELECT 1
      FROM memory.dds.lnk_customer_order l
      WHERE l.hk_lnk_co = to_hex(md5(to_utf8(
          'CO|' ||
          CAST(o.custkey AS varchar) || '|' ||
          CAST(o.orderkey AS varchar)
      )))
  );

-- =========================================================
-- TRANSACTIONAL LINK: ORDER–PART–SUPPLIER
-- (only lineitems of orders from the day)
-- =========================================================

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
JOIN tpch.tiny.orders o
  ON o.orderkey = li.orderkey
WHERE o.orderdate = DATE '${LOAD_DATE}'
  AND NOT EXISTS (
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
-- SATELLITE ON TRANSACTIONAL LINK (lineitem attributes)
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
    li.quantity,
    li.extendedprice,
    li.discount,
    li.tax,
    li.returnflag,
    li.linestatus,
    li.shipdate,
    li.commitdate,
    li.receiptdate,
    li.shipinstruct,
    li.shipmode,
    li.comment
FROM tpch.tiny.lineitem li
JOIN tpch.tiny.orders o
  ON o.orderkey = li.orderkey
WHERE o.orderdate = DATE '${LOAD_DATE}'
  AND NOT EXISTS (
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
