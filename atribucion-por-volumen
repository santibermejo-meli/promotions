SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.vectorized.execution.enabled = false;

WITH
vip AS (
        SELECT DISTINCT
         application.site_id                                                     AS site_id,
         device.platform                                                         AS platform,
         usr.uid                                                                 AS uid,
        IF(jest(event_data, 'item_id') IS NULL,
                 IF(jest(event_data, 'items') IS NULL,
                    jest(event_data, 'item_ids'),
                    jest(event_data, 'items')
                 ),
                  jest(event_data, 'item_id'))                                  AS item_id,
         IF(jest(platform.fragment,'c_id') = '/home/promotions/element', 'Carrousel', 'Landing') AS originAttribut,
         CONCAT(SUBSTR(server_timestamp,1,10),' ',SUBSTR(server_timestamp,12,8)) AS event_exact_date
        FROM tracks
        WHERE ds >= concat(cast((current_date - interval '2' day) as string), ' 00')
         and ds < concat(cast(current_date as string), ' 00')
         and path IN ('/vip' , '/pdp')
         AND usr.uid IS NOT NULL
         AND (
              jest(platform.fragment,'deal_print_id') IS NOT NULL
           OR jest(platform.fragment,'c_id') = '/home/promotions/element'
             )
       ),
       
test_customers as (
  select cus_cust_id
  from melilake.lk_cus_customers
  where is_test = 'true'
),
       
ord AS (
        SELECT DISTINCT
         application.site_id                                                     AS site_id,
         device.platform                                                         AS platform,
         jest(event_data,'items[0].item.id')                                     AS item_id,
         CAST(jest(event_data, 'order_id') AS DECIMAL(18,0))                     AS order_id,
         jest(event_data, 'status')                                              AS status,
         usr.uid                                                                 AS uid,
         jest(event_data, 'items[0].quantity')                                   AS item_quantity,
         jest(event_data, 'total_amount_usd')                                    AS gmv,
         CONCAT(SUBSTR(server_timestamp,1,10),' ',SUBSTR(server_timestamp,12,8)) AS order_exact_date,
         FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR(server_timestamp,1,10),' ',SUBSTR(server_timestamp,12,8)), 'yyyy-MM-dd HH:mm:ss') - 86400) as order_exact_date_sub_24h
        FROM tracks
        LEFT JOIN test_customers t
          ON usr.user_id = t.cus_cust_id
        WHERE path = '/orders/ordercreated'  
         and ds >= concat(cast((current_date - interval '1' day) as string), ' 00')
         and ds < concat(cast(current_date as string), ' 00') --genera el mismo dia del odr_created_dt
         and t.cus_cust_id is null
       )
       
SELECT
      site_id,
      platform,
      status,
      SUM(IF(qcarrousel > 0, CAST(GMVE AS DOUBLE), 0)) as GMV_CAR,
      SUM(IF(qcarrousel > 0, CAST(SI AS DOUBLE), 0)) as SI_CAR,
      SUM(IF(qlanding > 0, CAST(GMVE AS DOUBLE), 0)) as GMV_LAN,
      SUM(IF(qlanding > 0, CAST(SI AS DOUBLE), 0)) as SI_LAN,
      SUM(IF((qlanding + qcarrousel) > 0, CAST(GMVE AS DOUBLE), 0)) as GMV_AG,
      SUM(IF((qlanding + qcarrousel) > 0, CAST(SI AS DOUBLE), 0)) as SI_AG,
      SUM(CAST(SI AS DOUBLE))   AS SI_TS,
      SUM(CAST(GMVE AS DOUBLE)) AS GMVE_TS
FROM (
      SELECT
            ord.site_id                                   AS site_id,
            ord.platform                                  AS platform,
            ord.item_id                                   AS item_id,
            ord.order_id                                  AS order_id,
            ord.uid                                       AS uid,
            ord.status                                    AS status,
            ord.item_quantity                             AS SI,
            ord.gmv                                       AS GMVE,
            SUM(IF(coalesce(vip.originAttribut,'NA') = 'Carrousel',1,0)) AS qCarrousel,
            SUM(IF(coalesce(vip.originAttribut,'NA') = 'Landing',1,0))   AS qLanding
      FROM ord
      left join vip
        ON trim(vip.site_id  = ord.site_id
        AND trim(vip.platform = ord.platform
        AND trim(vip.item_id  = ord.item_id
        AND trim(vip.uid      = ord.uid
        AND vip.event_exact_date >= ord.order_exact_date_sub_24h
        AND vip.event_exact_date  < ord.order_exact_date
      GROUP BY ord.site_id, ord.platform, ord.item_id, ord.order_id, ord.uid, ord.status, ord.item_quantity, ord.gmv
    ) out1
GROUP BY site_id,
      platform,
      status
