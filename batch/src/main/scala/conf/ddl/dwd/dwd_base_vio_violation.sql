CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_base_vio_violation(
    wfbh string    COMMENT '违法编号',
    jdslb string    COMMENT '决定书类别',
    jdsbh string    COMMENT '决定书编号',
    wsjyw string    COMMENT '文书校验位',
    ryfl string    COMMENT '人员分类',
    jszh string    COMMENT '驾驶证号',
    dabh string    COMMENT '档案编号',
    fzjg string    COMMENT '发证机关',
    zjcx string    COMMENT '准驾车型',
    dsr string    COMMENT '当事人',
    zsxzqh string    COMMENT '住所行政区划',
    zsxxdz string    COMMENT '住所详细地址',
    dh string    COMMENT '电话',
    lxfs string    COMMENT '联系方式',
    clfl string    COMMENT '车辆分类',
    hpzl string    COMMENT '号牌种类',
    hphm string    COMMENT '号牌号码',
    jdcsyr string    COMMENT '机动车所有人',
    syxz string    COMMENT '机动车使用性质',
    jtfs string    COMMENT '交通方式',
    wfsj string    COMMENT '违法时间',
    xzqh string    COMMENT '行政区划',
    dllx string    COMMENT '道路类型',
    glxzdj string    COMMENT '公路行政等级',
    wfdd string    COMMENT '违法地点',
    lddm string    COMMENT '路口路段代码',
    ddms string    COMMENT '地点米数',
    ddjdwz string    COMMENT '地点绝对位置',
    wfdz string    COMMENT '违法地址',
    wfxw string    COMMENT '违法行为',
    wfjfs string    COMMENT '违法记分数',
    fkje string    COMMENT '罚款金额',
    scz string    COMMENT '实测值',
    bzz string    COMMENT '标准值',
    znj string    COMMENT '滞纳金',
    zqmj string    COMMENT '执勤民警',
    jkfs string    COMMENT '交款方式',
    fxjg string    COMMENT '发现机关',
    fxjgmc string    COMMENT '发现机关名称',
    cljg string    COMMENT '处理机关',
    cljgmc string    COMMENT '处理机关名称',
    cfzl string    COMMENT '处罚种类',
    clsj string    COMMENT '处理时间',
    jkbj string    COMMENT '交款标记',
    jkrq string    COMMENT '交款日期',
    pzbh string    COMMENT '强制措施凭证号',
    jsjqbj string    COMMENT '拒收拒签标记',
    jllx string    COMMENT '记录类型',
    lrr string    COMMENT '录入人',
    lrsj string    COMMENT '录入时间',
    jbr1 string    COMMENT '经办人1',
    jbr2 string    COMMENT '经办人2',
    sgdj string    COMMENT '事故等级',
    cldxbj string    COMMENT '处理对象标记',
    jdccldxbj string    COMMENT '机动车处理对象标记',
    zdjlbj string    COMMENT '转递记录标记',
    xxly string    COMMENT '信息来源',
    xrms string    COMMENT '写入模式',
    dkbj string    COMMENT '导库标记',
    jmznjbj string    COMMENT '减免滞纳金标记',
    zdbj string    COMMENT '转递标记',
    jsjg string    COMMENT '接受机关',
    fsjg string    COMMENT '发送机关',
    gxsj string    COMMENT '更新时间',
    bz string    COMMENT '备注',
    ywjyw string    COMMENT '业务校验位',
    zjmc string    COMMENT '证件名称',
    cclzrq string    COMMENT '初次领证日期',
    nl string    COMMENT '年龄',
    xb string    COMMENT '性别',
    hcbj string    COMMENT '核查标记',
    jd string    COMMENT '经度',
    wd string    COMMENT '维度',
    ylzz1 string    COMMENT '预留字段',
    ylzz2 string    COMMENT '预留字段',
    ylzz3 string    COMMENT '预留字段',
    ylzz4 string    COMMENT '预留字段',
    ylzz5 string    COMMENT '预留字段',
    ylzz6 string    COMMENT '存放支队工作库接受时间',
    ylzz7 string    COMMENT '预留字段',
    ylzz8 string    COMMENT '预留字段',
    cjfs string    COMMENT '采集方式',
    wfsj1 string    COMMENT '违法时间1',
    wfdd1 string    COMMENT '违法地点1',
    lddm1 string    COMMENT '路段代码1',
    ddms1 string    COMMENT '地点米数1',
    jsrxz string    COMMENT '驾驶人性质',
    clyt string    COMMENT '车辆用途',
    xcfw string    COMMENT '是否提供校车服务',
    dzzb string    COMMENT '电子坐标',
    sfzdry string    COMMENT '是否指导人员',
    xysfzmhm string    COMMENT '学员身份证明号码',
    xyxm string    COMMENT '学员姓名',
    ylzz11 string    COMMENT 'ylzz11',
    ylzz12 string    COMMENT 'ylzz12',
    ylzz13 string    COMMENT 'ylzz13',
    ylzz14 string    COMMENT 'ylzz14',
    ylzz15 string    COMMENT 'ylzz15',
    ylzz16 string    COMMENT 'ylzz16',
    ylzz17 string    COMMENT 'ylzz17',
    ylzz18 string    COMMENT 'ylzz18',
    qzfs string    COMMENT 'qzfs',
    sjhm string    COMMENT '手机号码',
    dcljg string    COMMENT 'dcljg',
    dcljgmc string    COMMENT 'dcljgmc',
    sfydcl string    COMMENT '是否异地车辆',
    dclfzjg string    COMMENT 'dclfzjg',
    dzdasfcj string   COMMENT 'dzdasfcj')
PARTITIONED BY
(
 ds   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
STORED AS textfile
location '/daas/motl/dwd/dwd_base_vio_violation/';
-- 添加分区
alter table dwd.dwd_base_vio_violation add PARTITION(ds="20230425");

-- 将数据存入到hdfs中 hdfs dfs -put dwd_base_vio_violation.log /daas/motl/dwd/dwd_base_vio_violation/ds=20230425

-- 检验数据 select * from dwd.dwd_base_vio_violation limit 10;