DROP TABLE bi.promotions_gmv;

CREATE EXTERNAL TABLE IF NOT EXISTS bi.promotions_gmv (
FechaHora string,
hora string,
sit_site_id string,
category_L1 string,
category_L2 string,
category_L3 string,
category_name_L1 string,
category_name_L2 string,
category_name_L3 string,
platform string,
landing int,
atr_type string,
gmve float,
SI decimal(38,0)
)
PARTITIONED BY (
 fecha string)
ROW FORMAT SERDE
 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'
STORED AS INPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'
OUTPUTFORMAT
 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'
LOCATION
 's3://melidata-results-batch/promotions/historic_gmv';
 
grant select on bi.promotions_gmv to user etl_bi;
