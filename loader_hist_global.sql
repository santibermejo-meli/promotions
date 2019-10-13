DROP TABLE BI.ATTRIB_EVENTS_GLOBAL;

--CREO TABLA TEMPORAL
CREATE TABLE BI.ATTRIB_EVENTS_GLOBAL
(
SIT_SITE_ID  STRING,
    UID  STRING,
    ITE_ITEM_ID  STRING,
    PLATFORM  STRING,
    EVENT_EXACT_DATE  STRING,
    LANDING  STRING
)   STORED AS PARQUET
LOCATION 's3://melidata-results-batch/promotions/attrib_events_global/';

INSERT INTO TABLE BI.ATTRIB_EVENTS_GLOBAL
    SELECT
    distinct application.site_id as sit_site_id,
    usr.uid as uid,
    IF(get_json_object(event_data, '$.item_id') IS NULL,
        IF(get_json_object(event_data, '$.items') IS NULL,
              get_json_object(event_data, '$.item_ids'),
              get_json_object(event_data, '$.items')
    ),get_json_object(event_data, '$.item_id')) as ite_item_id,
    device.platform as platform,
    CONCAT(SUBSTR(server_timestamp,0,10),' ',SUBSTR(server_timestamp,12,8)) as event_exact_date,
    CASE WHEN get_json_object(platform.fragment, '$.c_id') = '/home/promotions/element' THEN 0 ELSE 1 END as landing
    FROM tracks
    WHERE  path = '/vip'
    and ds >=  cast(DATE_SUB(<fecha_analisis>,2) as string) 
    and ds <   cast(<fecha_analisis> as string) 
    and usr.uid is not null
    and (get_json_object(platform.fragment, '$.deal_print_id') is not null or get_json_object(platform.fragment, '$.c_id') = '/home/promotions/element');

ALTER TABLE bi.promotions_gmv SET TBLPROPERTIES('EXTERNAL'='FALSE');

-- GMV Atribuido Global

INSERT INTO TABLE bi.promotions_gmv PARTITION (fecha)
SELECT
      CONCAT(cast(bids.TIM_DAY_WINNING_DATE as string),' ',SUBSTR(CAST(bids.tim_time_winning_date+1000000 AS string),2,2)) AS FechaHora,
      SUBSTR(CAST(bids.tim_time_winning_date+1000000 AS string),2,2) AS hora,
      bids.sit_site_id as sit_site_id,
      bids.CAT_CATEG_ID_L1 AS category_L1,
      bids.CAT_CATEG_ID_L2 AS category_L2,
      bids.CAT_CATEG_ID_L3 AS category_L3,
      bids.CAT_CATEG_NAME_L1 AS category_name_L1,
      bids.CAT_CATEG_NAME_L2 AS category_name_L2,
      bids.CAT_CATEG_NAME_L3 AS category_name_L3,
      at.platform,
      at.landing,
      '24 HOURS' as atr_type,
      sum(bids.bid_base_current_price * bids.bid_quantity_ok) as GMVE,
      sum(bids.bid_quantity_ok) as SI,
      bids.TIM_DAY_WINNING_DATE AS fecha
from melilake.bt_bids bids
INNER JOIN
    (SELECT
       application.site_id as sit_site_id,
       cast(get_json_object(event_data, '$.order_id') AS DECIMAL(18,0)) as ord_order_id,
       usr.uid as uid,
       CONCAT(SUBSTR(server_timestamp,0,10),' ',SUBSTR(server_timestamp,12,8)) as order_exact_date,
       from_unixtime(unix_timestamp(CONCAT(SUBSTR(server_timestamp,0,10),' ',SUBSTR(server_timestamp,12,8)), 'yyyy-MM-dd HH:mm:ss') - 86400) as order_exact_date_sub_24h
    FROM default.tracks
    WHERE ds >=  concat(cast(DATE_SUB(<fecha_analisis>,1) as string), ' 00')
    and ds <= concat(<fecha_analisis>, ' 00') --genera el mismo dia del odr_created_dt
    and path = '/orders/ordercreated' ) orders
  on bids.ord_order_id = orders.ord_order_id
INNER JOIN
       (select  sit_site_id,
                 uid,
                 platform,
                 ite_item_id,
                 max(event_exact_date) as event_exact_date,
                 99 as landing
        from BI.ATTRIB_EVENTS_GLOBAL
        group by sit_site_id,
                 uid,
                 platform,
                 ite_item_id,
                 max(event_exact_date),
                 99
        ) at
        ON (
           at.sit_site_id = orders.sit_site_id
          AND at.uid = orders.uid
          AND at.event_exact_date >= order_exact_date_sub_24h
          AND at.event_exact_date <  order_exact_date
          AND at.ite_item_id = trim(concat(bids.sit_site_id,cast(bids.ite_item_id as varchar)))
WHERE bids.ite_gmv_flag = 1
AND bids.mkt_marketplace_id = 'TM'
AND bids.ord_created_dt  = DATE_SUB(<fecha_analisis>,1) 
and bids.tim_day_winning_date is not null
AND bids.ite_today_promotion_flag = 1
GROUP BY CONCAT(cast(bids.TIM_DAY_WINNING_DATE as string),' ',SUBSTR(CAST(bids.tim_time_winning_date+1000000 AS string),2,2)),
      SUBSTR(CAST(bids.tim_time_winning_date+1000000 AS string),2,2),
      bids.sit_site_id,
      bids.CAT_CATEG_ID_L1,
      bids.CAT_CATEG_ID_L2,
      bids.CAT_CATEG_ID_L3,
      bids.CAT_CATEG_NAME_L1,
      bids.CAT_CATEG_NAME_L2,
      bids.CAT_CATEG_NAME_L3,
      at.platform,
      at.landing,
      '24 HOURS',
      bids.TIM_DAY_WINNING_DATE;

ALTER TABLE bi.promotions_gmv SET TBLPROPERTIES('EXTERNAL'='TRUE');

--BORRO TABLE TEMPORAL
DROP TABLE BI.ATTRIB_EVENTS_GLOBAL;
