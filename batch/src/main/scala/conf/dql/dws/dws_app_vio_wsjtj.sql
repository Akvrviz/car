with sbbh_day_sjc as(
    select
        sbbh,
        wfsj_day,
        after_day,
        datediff(after_day, wfsj_day) as sjc, -- 时间差
        row_number() over(partition by sbbh order by datediff(after_day, wfsj_day)) as sjcpm --时间差排名
    from (
        select -- 获取设备编号，前一次违法日期，后一次违法日期
        sbbh,
        wfsj_day,
        lead(wfsj_day, 1, current_date) over(partition by sbbh order by wfsj_day) as after_day
        from(
            select
            sbbh,
            substr(wfsj, 1, 10) as wfsj_day
            from dwd.dwd_base_vio_surveil
            where sbbh is not null and sbbh != "" and ds = "20230425"
            group by sbbh, substr(wfsj, 1, 10)
        ) as a
    ) as b
), sbbh_q1q3 as (
    select -- 计算Q1 Q3
        sbbh,
        floor(count(*)/4) as Q1,
        ceil(count(*)*3/4) as Q3
    from sbbh_day_sjc
    group by sbbh
), sbbh_q1q3_value as (
    select
        sbbh,
        sum(Q1) as Q1,
        sum(Q3) as Q3,
        sum(Q1_value) as Q1_value,
        sum(Q3_value) as Q3_value
    from(
        select
            c.sbbh as sbbh,
            c.wfsj_day as wfsj_day,
            c.after_day as after_day,
            c.sjc as  sjc,
            if(c.sjcpm = d.Q1, Q1, 0) as Q1,
            if(c.sjcpm = d.Q3, Q3, 0) as Q3,
            if(c.sjcpm = d.Q1, c.sjc, 0) as Q1_value,
            if(c.sjcpm = d.Q3, c.sjc, 0) as Q3_value
        from sbbh_day_sjc as c left join sbbh_q1q3 as d on c.sbbh = d.sbbh
    ) as e
    group by sbbh
)
insert overwrite table dws.dws_app_vio_wsjtj partition(ds="20230425")
select
    t1.sbbh as sbbh,
    t1.sjc as sjc,
    t1.wfsj_day as wfsj_day,
    t1.after_day as  after_day,
    round(t1.sjc/ceil(t2.Q3_value + (t2.Q3_value - t2.Q1_value)*1.5),2) as more_zb
from sbbh_day_sjc as t1 join sbbh_q1q3_value as t2 on t1.sbbh = t2.sbbh
where ceil(t2.Q3_value + (t2.Q3_value - t2.Q1_value)*1.5) < t1.sjc;