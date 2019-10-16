SET hive.exec.dynamic.partition = true;
SET hive.exec.dynamic.partition.mode = nonstrict;
SET hive.vectorized.execution.enabled = false;

CREATE TEMPORARY TABLE
bid AS (
        SELECT
          CONCAT(cast(a11.TIM_DAY_WINNING_DATE as string),' ',SUBSTR(CAST(a11.tim_time_winning_date+1000000 AS string),2,2)) AS FechaHora,
          SUBSTR(CAST(a11.tim_time_winning_date+1000000 AS string),2,2) AS hora,
          trim(a11.SIT_SITE_ID)                                               AS sit_site_id,
          trim(a11.CAT_CATEG_ID_L1)                                   AS category_L1,
          a11.CAT_CATEG_ID_L2                                           AS category_L2,
          a11.CAT_CATEG_ID_L3                                           AS category_L3,
          a11.CAT_CATEG_NAME_L1                                         AS category_name_L1,
          a11.CAT_CATEG_NAME_L2                                         AS category_name_L2,
          a11.CAT_CATEG_NAME_L3                                         AS category_name_L3,
          a11.ORD_order_id                                              AS order_id,
          a11.ORD_created_dt                                            AS order_create_date,
          DATE_SUB(a11.ORD_created_dt,1)                                AS order_create_date24h,
          trim(concat(a11.sit_site_id,cast(a11.ite_item_id as string))) AS item_id,
          IF(a11.ite_today_promotion_flag = 1, a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK, 0) AS GMVE_AG,
          IF(a11.ite_today_promotion_flag = 1, a11.BID_QUANTITY_OK, 0) AS SI_AG,
          a11.BID_BASE_CURRENT_PRICE * a11.BID_QUANTITY_OK AS GMVE_TS,
          a11.BID_QUANTITY_OK   AS SI_TS
        FROM melilake.BT_BIDS a11
        WHERE a11.TIM_DAY_WINNING_DATE = DATE_SUB(CURRENT_DATE,1)
          AND a11.ITE_GMV_FLAG       = 1
          AND a11.MKT_MARKETPLACE_ID = 'TM'
       );

CREATE TEMPORARY TABLE
vip AS (
        SELECT DISTINCT
         application.site_id                                                     AS site_id,
         device.platform                                                         AS platform,
         usr.uid                                                                 AS uid,
         jest(event_data, 'item_id')                                             AS item_id,
         IF(jest(platform.fragment,'c_id') = '/home/promotions/element', 'Carrousel', 'Landing') AS originAttribut,
         CONCAT(SUBSTR(server_timestamp,1,10),' ',SUBSTR(server_timestamp,12,8)) AS event_exact_date
        FROM tracks
        WHERE  path = '/vip'
         AND ds >= concat(cast(DATE_SUB(CURRENT_DATE,8) as string), ' 00')
         AND ds >= concat(cast(DATE_SUB(CURRENT_DATE,1) as string), ' 00')
         AND usr.uid IS NOT NULL
         AND (
              jest(platform.fragment,'deal_print_id') IS NOT NULL
           OR jest(platform.fragment,'c_id') = '/home/promotions/element'
             )
       );
       

CREATE TEMPORARY TABLE
ord AS (
        SELECT DISTINCT
         application.site_id                                                     AS site_id,
         device.platform                                                         AS platform,
         jest(event_data,'items[0].item.id')                                     AS item_id,
         CAST(jest(event_data, 'order_id') AS DECIMAL(18,0))                     AS order_id,
         usr.uid                                                                 AS uid,
         CONCAT(SUBSTR(server_timestamp,1,10),' ',SUBSTR(server_timestamp,12,8)) AS order_exact_date,    
         FROM_UNIXTIME(UNIX_TIMESTAMP(CONCAT(SUBSTR(server_timestamp,1,10),' ',SUBSTR(server_timestamp,12,8)), 'yyyy-MM-dd HH:mm:ss') - 86400) as order_exact_date_sub_24h
        FROM tracks
        WHERE path = '/orders/ordercreated'  
         AND ds >= concat(cast(DATE_SUB(CURRENT_DATE,7) as string), ' 00')
         AND ds <= concat(CURRENT_DATE, ' 00')
       );
       
    
CREATE TEMPORARY TABLE
vxo AS (
        SELECT
              ord.site_id                                   AS site_id,
              ord.platform                                  AS platform,
              ord.item_id                                   AS item_id,
              ord.order_id                                  AS order_id,
              SUM(IF(vip.originAttribut = 'Carrousel',1,0)) AS qCarrousel,
              SUM(IF(vip.originAttribut = 'Landing',1,0))   AS qLanding
        FROM vip, ord
        WHERE trim(vip.site_id)  = trim(ord.site_id)
          AND trim(vip.platform) = trim(ord.platform)
          AND trim(vip.item_id)  = trim(ord.item_id)
          AND trim(vip.uid)      = trim(ord.uid)
          AND  cast(vip.event_exact_date as timestamp)  >= cast(ord.order_exact_date_sub_24h as timestamp)
          AND  cast(vip.event_exact_date as timestamp)  < cast(ord.order_exact_date as timestamp)
        GROUP BY ord.site_id, ord.platform, ord.item_id, ord.order_id
       );

SELECT
      bid.FechaHora                                                AS FechaHora,
      bid.hora                                                     AS hora,
      bid.sit_site_id                                              AS sit_site_id,
      bid.category_L1                                              AS category_L1,
      bid.category_L2                                              AS category_L2,
      bid.category_L3                                              AS category_L3,
      bid.category_name_L1                                         AS category_name_L1,
      bid.category_name_L2                                         AS category_name_L2,
      bid.category_name_L3                                         AS category_name_L3,
      SUM(bid.GMVE_AG)                                             AS GMVE_AG,
      SUM(bid.SI_AG)                                               AS SI_AG,
      SUM(bid.GMVE_TS)                                             AS GMVE_TS,
      SUM(bid.SI_TS)                                               AS SI_TS,
      SUM(IF(vxo.qLanding > 0, bid.GMVE_TS, 0))                    AS GMVE_LAN,
      SUM(IF(vxo.qLanding > 0, bid.SI_TS, 0))                      AS SI_LAN,
      SUM(IF(vxo.qCarrousel > 0, bid.GMVE_TS, 0))                  AS GMVE_CAR,
      SUM(IF(vxo.qCarrousel > 0, bid.SI_TS, 0))                    AS SI_CAR,
      SUM(IF((vxo.qLanding + vxo.qCarrousel) > 0, bid.GMVE_TS, 0)) AS GMVE_TA,
      SUM(IF((vxo.qLanding + vxo.qCarrousel) > 0, bid.SI_TS, 0))   AS SI_TA
FROM bid
LEFT JOIN vxo
ON bid.order_id = vxo.order_id
GROUP BY bid.FechaHora,
bid.hora,
bid.sit_site_id,
bid.category_L1, 
bid.category_L2,
bid.category_L3,
bid.category_name_L1,
bid.category_name_L2,
bid.category_name_L3



/*
  select bid.sit_site_id,
  bid.fechahora,
        SUM(bid.GMVE_AG)                                             AS GMVE_AG,
        SUM(bid.SI_AG)                                               AS SI_AG,
        SUM(bid.GMVE_TS)                                             AS GMVE_TS,
        SUM(bid.SI_TS)                                               AS SI_TS,
        SUM(IF(vxo.qLanding > 0, bid.GMVE_TS, 0))                    AS GMVE_LAN,
        SUM(IF(vxo.qLanding > 0, bid.SI_TS, 0))                      AS SI_LAN,
        SUM(IF(vxo.qCarrousel > 0, bid.GMVE_TS, 0))                  AS GMVE_CAR,
        SUM(IF(vxo.qCarrousel > 0, bid.SI_TS, 0))                    AS SI_CAR,
        SUM(IF((vxo.qLanding + vxo.qCarrousel) > 0, bid.GMVE_TS, 0)) AS GMVE_TA,
        SUM(IF((vxo.qLanding + vxo.qCarrousel) > 0, bid.SI_TS, 0))   AS SI_TA
  FROM bid
  LEFT JOIN vxo
  ON trim(cast(bid.order_id as string)) = trim(cast(vxo.order_id as string))
  GROUP BY bid.sit_site_id,
  bid.fechahora
*/
