CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_app_acd_mrjq(
    tjrq STRING COMMENT '统计日期',

    dr_sgs int COMMENT '当日事故数',
    jn_sgs int COMMENT '今年事故数',
    tb_sgs double COMMENT '同比事故数', -- 按年同比？按月同比？按天同比？
    tb_sgs_bj STRING COMMENT '同比事故数标记', -- 标记区分年月日？

    dr_swsgs int COMMENT '当日死亡事故数', -- 当天有死亡的事故数
    jn_swsgs int COMMENT '今年死亡事故数', -- 当年有死亡的事故数
    tb_swsgs double COMMENT '同比死亡事故数',
    tb_swsgs_bj STRING COMMENT '同比死亡事故数标记',

    dr_swrs int COMMENT '当日死亡人数', --
    jn_swrs int COMMENT '今年死亡人数',
    tb_swrs double COMMENT '同比死亡人数',
    tb_swrs_bj STRING COMMENT '同比死亡人数标记',

    dr_ssrs int COMMENT '当日受伤人数',
    jn_ssrs int COMMENT '今年受伤人数',
    tb_ssrs double COMMENT '同比受伤人数',
    tb_ssrs_bj STRING COMMENT '同比受伤人数标记',

    dr_zjccss int COMMENT '当日财产损失',
    jn_zjccss int COMMENT '今年财产损失',
    tb_zjccss double COMMENT '同比财产损失',
    tb_zjccss_bj STRING COMMENT '同比财产损失标记')
PARTITIONED BY
(
 ds   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
STORED AS textfile
location '/daas/motl/dws/dws_app_acd_mrjq/';