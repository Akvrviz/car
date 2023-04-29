insert overwrite table dws.dws_app_acd_mrjq partition(ds="20230425") -- 日期可换成变量
select
    tjrq,

    dr_sgs,
    jn_sgs,
    round(((dr_sgs-qntq_sgs)/qntq_sgs)*100, 2) as tb_sgs,
    if(dr_sgs-qntq_sgs>0,"上升","下降") as tb_sgs_bj,

    dr_swsgs,
    jn_swsgs,
    round(((dr_swsgs-qntq_swsgs)/qntq_swsgs)*100, 2) as tb_swsgs,
    if(dr_swsgs-qntq_swsgs>0,"上升","下降") as tb_swsgs_bj,

    dr_swrs,
    jn_swrs,
    round(((dr_swrs-qntq_swrs)/qntq_swrs)*100, 2) as tb_swrs,
    if(dr_swrs-qntq_swrs>0,"上升","下降") as tb_swrs_bj,

    dr_ssrs,
    jn_ssrs,
    round(((dr_ssrs-qntq_ssrs)/qntq_ssrs)*100, 2) as tb_ssrs,
    if(dr_ssrs-qntq_ssrs>0,"上升","下降") as tb_ssrs_bj,

    dr_zjccss,
    jn_zjccss,
    round(((dr_zjccss-qntq_zjccss)/qntq_zjccss)*100, 2) as tb_zjccss,
    if(dr_zjccss-qntq_zjccss>0,"上升","下降") as tb_zjccss_bj
from(
    select
    tjrq,

    dr_sgs, -- 当日事故数
    sum(dr_sgs) over(partition by year(tjrq)) as jn_sgs, -- 当年事故数
    lag(dr_sgs, 1, 1) over(partition by substr(tjrq, 6, 10) order by year(tjrq)) as qntq_sgs, --去年同期事故数

    dr_swrs, -- 当日死亡人数
    sum(dn_ssrs) over(partition by year(tjrq)) as jn_swrs,
    lag(dr_swrs, 1, 1) over(partition by substr(tjrq, 6, 10) order by year(tjrq)) as qntq_swrs,

    dr_swsgs, -- 当日死亡事故数
    sum(dn_swsgs) over(partition by year(tjrq)) as jn_swsgs,
    lag(dr_swsgs, 1, 1) over(partition by substr(tjrq, 6, 10) order by year(tjrq)) as qntq_swsgs,

    dr_ssrs, -- 当日受伤人数
    sum(dn_ssrs) over(partition by year(tjrq)) as jn_ssrs,
    lag(dr_ssrs, 1, 1) over(partition by substr(tjrq, 6, 10) order by year(tjrq)) as qntq_ssrs,

    dr_zjccss, -- 当日直接财产损失
    sum(dr_zjccss) over(partition by year(tjrq)) as jn_zjccss,
    lag(dr_zjccss, 1, 1) over(partition by substr(tjrq, 6, 10) order by year(tjrq)) as qntq_zjccss
    from(
        select
            substr(sgfssj, 0, 10) as tjrq,
            count(1) as dr_sgs, -- 当日事故数
            sum(if(swrs24 > 0 , 1, 0)) as dr_swsgs, --当日死亡事故数
            sum(if(swrs30 > 0, 1, 0)) as dn_swsgs, -- 当天所有事故中，30天内出现死亡的事故数
            sum(swrs24) as dr_swrs, -- 当天所有事故中，当日死亡人数
            sum(swrs30) as dn_swrs, -- 当天所有事故中，30天内死亡的人数
            sum(ssrs24) as dr_ssrs, -- 当日受伤人数
            sum(ssrs30) as dn_ssrs, -- 当年受伤人数
            sum(zjccss) as dr_zjccss -- 当日财产损失
        from
            dwd.dwd_base_acd_file
        where ds = "20230425" -- 可换成变量
        group by
            substr(sgfssj,0,10)
        ) as a
    ) as b;