CREATE EXTERNAL TABLE IF NOT EXISTS dws.dws_app_vio_zqzf(
    tjrq STRING COMMENT '统计日期',

    -- 每4个一组， 共11组
    dr_zd_wfs int COMMENT '当日违法总数',
    jn_zd_wfs int COMMENT '今年违法总数',
    zd_tb double COMMENT '同比',
    zd_tbbj STRING COMMENT '同比标记',

    dr_zjjs_wfs int COMMENT '当日醉酒驾驶违法数',
    jn_zjjs_wfs int COMMENT '今年醉酒驾驶违法数',
    zjjs_tb double COMMENT '醉酒驾驶同比',
    zjjs_tbbj STRING COMMENT '醉酒驾驶标记',

    dr_yjjs_wfs int COMMENT '当日饮酒驾驶违法数',
    jn_yjjs_wfs int COMMENT '今年饮酒驾驶违法数',
    yjjs_tb double COMMENT '饮酒驾驶占比',
    yjjs_tbbj STRING COMMENT '饮酒驾驶标记',

    dr_zdhp_wfs int COMMENT '当日未挂、遮挡、污损违法数',
    jn_zdhp_wfs int COMMENT '今年未挂、遮挡、污损违法数',
    zdhp_tb double COMMENT '未挂、遮挡、污损占比',
    zdhp_tbbj STRING COMMENT '未挂、遮挡、污损标记',

    dr_wzbz_wfs int COMMENT '当日伪造变造违法数',
    jn_wzbz_wfs int COMMENT '今年伪造变造违法数',
    wzbz_tb double COMMENT '伪造变造占比',
    wzbz_tbbj STRING COMMENT '伪造变造标记',

    dr_tpjp_wfs int COMMENT '当日套牌车违法数',
    jn_tpjp_wfs int COMMENT '今年套牌车违法数',
    tpjp_tb double COMMENT '套牌车占比',
    tpjp_tbbj STRING COMMENT '套牌车标记',

    dr_wdtk_wfs int COMMENT '当日未戴头盔违法数',
    jn_wdtk_wfs int COMMENT '今年未戴头盔违法数',
    wdtk_tb double COMMENT '未戴头盔占比',
    wdtk_tbbj STRING COMMENT '未戴头盔标记',

    dr_jspz_wfs int COMMENT '当日驾驶拼装报废机动车违法数',
    jn_jspz_wfs int COMMENT '今年驾驶拼装报废机动车违法数',
    jspz_tb double COMMENT '驾驶拼装报废机动车占比',
    jspz_tbbj STRING COMMENT '驾驶拼装报废机动车标记',

    dr_wxjs_wfs int COMMENT '当日无有效驾驶资格违法数',
    jn_wxjs_wfs int COMMENT '今年无有效驾驶资格违法数',
    wxjs_tb double COMMENT '无有效驾驶资格占比',
    wxjs_tbbj STRING COMMENT '无有效驾驶资格标记',

    dr_cy_wfs int COMMENT '当日超员违法数',
    jn_cy_wfs int COMMENT '今年超员违法数',
    cy_tb double COMMENT '超员占比',
    cy_tbbj STRING COMMENT '超员标记',

    dr_cz_wfs int COMMENT '当日超载违法数',
    jn_cz_wfs int COMMENT '今年超载违法数',
    cz_tb double COMMENT '超载占比',
    cz_tbbj STRING COMMENT '超载标记')
PARTITIONED BY
(
    ds   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
STORED AS textfile
location '/daas/motl/dws/dws_app_vio_zqzf/';
