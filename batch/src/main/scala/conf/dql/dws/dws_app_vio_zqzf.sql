insert overwrite table dws.dws_app_vio_zqzf partition(ds="20230425") -- 日期可换成变量
select -- 根据两年同期值计算同比和标志
    tjrq,

    dr_zd_wfs,
    jn_zd_wfs,
    round(NVL((dr_zd_wfs - qntq_zd_wfs)/qntq_zd_wfs,0) * 100, 2) as zd_tb,
    if(dr_zd_wfs - qntq_zd_wfs > 0,'上升','下降') as zd_tbbj,

    dr_zjjs_wfs,
    jn_zjjs_wfs,
    round(NVL((dr_zjjs_wfs - qntq_zjjs_wfs)/qntq_zd_wfs,0) * 100, 2) as zjjs_tb,
    if(dr_zjjs_wfs - qntq_zjjs_wfs > 0,'上升','下降') as zjjs_tbbj,

    dr_yjjs_wfs,
    jn_yjjs_wfs,
    round(NVL((dr_yjjs_wfs - qntq_yjjs_wfs)/qntq_yjjs_wfs,0) * 100, 2) as yjjs_tb,
    if(dr_yjjs_wfs - qntq_yjjs_wfs > 0,'上升','下降') as yjjs_tbbj,

    dr_zdhp_wfs,
    jn_zdhp_wfs,
    round(NVL((dr_zdhp_wfs - qntq_zdhp_wfs)/qntq_zdhp_wfs,0) * 100, 2) as zdhp_tb,
    if(dr_zdhp_wfs - qntq_zdhp_wfs > 0,'上升','下降') as zdhp_tbbj,

    dr_wzbz_wfs,
    jn_wzbz_wfs,
    round(NVL((dr_wzbz_wfs - qntq_wzbz_wfs)/qntq_wzbz_wfs,0) * 100, 2) as wzbz_tb,
    if(dr_wzbz_wfs - qntq_wzbz_wfs > 0,'上升','下降') as wzbz_tbbj,

    dr_tpjp_wfs,
    jn_tpjp_wfs,
    round(NVL((dr_tpjp_wfs - qntq_tpjp_wfs)/qntq_tpjp_wfs,0) * 100, 2) as tpjp_tb,
    if(dr_tpjp_wfs - qntq_tpjp_wfs > 0,'上升','下降') as tpjp_tbbj,

    dr_wdtk_wfs,
    jn_wdtk_wfs,
    round(NVL((dr_wdtk_wfs - qntq_wdtk_wfs)/qntq_wdtk_wfs,0) * 100, 2) as wdtk_tb,
    if(dr_wdtk_wfs - qntq_wdtk_wfs > 0,'上升','下降') as wdtk_tbbj,

    dr_jspz_wfs,
    jn_jspz_wfs,
    round(NVL((dr_jspz_wfs - qntq_jspz_wfs)/qntq_jspz_wfs,0) * 100, 2) as jspz_tb,
    if(dr_jspz_wfs - qntq_jspz_wfs > 0,'上升','下降') as jspz_tbbj,

    dr_wxjs_wfs,
    jn_wxjs_wfs,
    round(NVL((dr_wxjs_wfs - qntq_wxjs_wfs)/qntq_wxjs_wfs,0) * 100, 2) as wxjs_tb,
    if(dr_wxjs_wfs - qntq_wxjs_wfs > 0,'上升','下降') as wxjs_tbbj,

    dr_cy_wfs,
    jn_cy_wfs,
    round(NVL((dr_cy_wfs - qntq_cy_wfs)/qntq_cy_wfs,0) * 100, 2) as cy_tb,
    if(dr_cy_wfs - qntq_cy_wfs > 0,'上升','下降') as cy_tbbj,

    dr_cz_wfs,
    jn_cz_wfs,
    round(NVL((dr_cz_wfs - qntq_cz_wfs)/qntq_cz_wfs,0) * 100, 2) as cz_tb,
    if(dr_cz_wfs - qntq_cz_wfs > 0,'上升','下降') as cz_tbbj
from(
    select -- 在细粒度数据（当日）基础上获取粗粒度数据（当年）
    tjrq,

    dr_zd_wfs,
    sum(dr_zd_wfs) over(partition by year(tjrq)) as jn_zd_wfs,
    lag(dr_zd_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_zd_wfs,

    dr_zjjs_wfs,
    sum(dr_zjjs_wfs) over(partition by year(tjrq)) as jn_zjjs_wfs,
    lag(dr_zjjs_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_zjjs_wfs,

    dr_yjjs_wfs,
    sum(dr_yjjs_wfs) over(partition by year(tjrq)) as jn_yjjs_wfs,
    lag(dr_yjjs_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_yjjs_wfs,

    dr_zdhp_wfs,
    sum(dr_zdhp_wfs) over(partition by year(tjrq)) as jn_zdhp_wfs,
    lag(dr_zdhp_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_zdhp_wfs,

    dr_wzbz_wfs,
    sum(dr_wzbz_wfs) over(partition by year(tjrq)) as jn_wzbz_wfs,
    lag(dr_wzbz_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_wzbz_wfs,

    dr_tpjp_wfs,
    sum(dr_tpjp_wfs) over(partition by year(tjrq)) as jn_tpjp_wfs,
    lag(dr_tpjp_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_tpjp_wfs,

    dr_wdtk_wfs,
    sum(dr_wdtk_wfs) over(partition by year(tjrq)) as jn_wdtk_wfs,
    lag(dr_wdtk_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_wdtk_wfs,

    dr_jspz_wfs,
    sum(dr_jspz_wfs) over(partition by year(tjrq)) as jn_jspz_wfs,
    lag(dr_jspz_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_jspz_wfs,

    dr_wxjs_wfs,
    sum(dr_wxjs_wfs) over(partition by year(tjrq)) as jn_wxjs_wfs,
    lag(dr_wxjs_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_wxjs_wfs,

    dr_cy_wfs,
    sum(dr_cy_wfs) over(partition by year(tjrq)) as jn_cy_wfs,
    lag(dr_cy_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_cy_wfs,

    dr_cz_wfs,
    sum(dr_cz_wfs) over(partition by year(tjrq)) as jn_cz_wfs,
    lag(dr_cz_wfs, 1, 1)over(partition by substr(tjrq, 6, 5) order by year(tjrq)) as qntq_cz_wfs
    from(
        select  -- 获取细粒度数据
        substr(wfsj, 0 ,10) as tjrq, -- 统计日期
        count(1) as dr_zd_wfs, -- 当日违法数
        -- 醉酒
        sum(if(wfxw in ("20011","20021","1702","1703","6022","5049","2010","20102","20010","20020","17021","17031","6032","6033","10043","A01"), 1, 0)) as dr_zjjs_wfs,
        -- 饮酒
        sum(if(wfxw in ("20061","20071","1711","1712","1713","6022","A17","20060","20070","17122","17121","1605","1604","17111","6034","6035","60351","S03","10043"), 1, 0)) as dr_yjjs_wfs,
        -- 当日未挂、遮挡、污损违法
        sum(if(wfxw in ("20211","1614","1329","20210","1718","17181","1331","1616","17201","1720"),1 ,0)) as dr_zdhp_wfs,
        -- 当日伪造变造违法数
        sum(if(wfxw in ("75011","5702","5703","5003","5006","5007","5010","5011","5009","57031","57032","50101","50102","50041","50042","50051","50052","50012","50011","50062","50061","50072","50071","50112","50111","5001","50031","5004","5005","5012","57022","57021","50032","60021","57011","57012","5054","5706","5601","5701","5002","50021","5008","50022"),1,0)) as dr_wzbz_wfs,
        -- 当日套牌车违法数
        sum(if(wfxw in ("57011","57012","5054","5706","5601","5701","5002","50021","5008","50022"),1,0)) as dr_tpjp_wfs,
        -- 当日未戴头盔违法数
        sum(if(wfxw in ("3020","30201","1207","20301","12071","74021"),1,0)) as dr_wdtk_wfs,
        -- 当日驾驶拼装报废机动车违法数
        sum(if(wfxw in ("6021","5051","10022","1002","20671"),1,0)) as dr_jspz_wfs,
        -- 当日无有效驾驶资格违法数
        sum(if(wfxw in ("6026","20111","10072","10062","5703","1010","5006","20032","10052","16103","10064","1015","10151","1610","1709","1009","10091","10061","10141","1014","57031","57032","1005","1006","10104","17091","10051","16102","16104","16101","50062","50061","1704","10821","1082","10101","10103","20110","5012","17094","17093","10054","10053","10063","10102","17092","60063","60061","60062","60021"),1,0)) as dr_wxjs_wfs,
        -- 当日超员违法数
        sum(if(wfxw in ("1710","20081","20082","1601","17101","17103","1202","1621","16211","17102","17104","20231","20412""6038","1238","1348","13481","71171","1241","1341","1714","1623","1626","16261","1627","17161","6017","60171","1716","16271"),1,0)) as dr_cy_wfs,
        -- 当日超载违法数
        sum(if(wfxw in ("13542","1342","1201","13532","1637","71181","13534","13544","13543","13541","16391","16393","16392","16394","12011","1602","13421","13422","12012","13533","13531","1353","20241","1639","16371","16373","16374","16372","1354","1346","1239","1608"),1,0)) as dr_cz_wfs
        from(
            select  --现场违法表
                substr(fxjg,1,4) as fxjg,
                wfxw,
                jtfs,
                dllx,
                xxly,
                wfdd,
                wfsj,
                cjbj,
                jllx
            from dwd.dwd_base_vio_force lateral view explode(array(wfxw1,wfxw2,wfxw3,wfxw4,wfxw5)) a as wfxw
            where wfxw is not null and unix_timestamp(substr(wfsj,0,10), 'yyyy-MM-dd') is not null -- 过滤违法行为为空的记录和时间格式异常的记录
            and ds = "20230425" -- 可换成变量
            union all
            select  -- 非现场违法
                substr(fxjg,1,4) as fxjg,
                wfxw,
                jtfs,
                dllx,
                xxly,
                wfdd,
                wfsj,
                "1" as cjbj,
                jllx
            from dwd.dwd_base_vio_violation
            where wfxw is not null and unix_timestamp(substr(wfsj,0,10), 'yyyy-MM-dd') is not null
            and ds = "20230425" -- 可换成变量
        ) as b
        group by substr(wfsj, 0, 10)
    ) as c
) as d;