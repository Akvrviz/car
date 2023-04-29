CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_app_vio_wsjtj(
sbbh STRING COMMENT '设备编号',
sjc int COMMENT '无数据天数',
wfsj_day STRING COMMENT '有数据日期',
after_day STRING COMMENT '下一次有数据日期',
more_zb double COMMENT '超过占比'
) COMMENT '无数据统计'
PARTITIONED BY
(
 ds   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
STORED AS textfile
location '/daas/motl/dws/dws_app_vio_wsjtj/';