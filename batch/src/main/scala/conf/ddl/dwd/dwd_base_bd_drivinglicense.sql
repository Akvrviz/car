-- 驾驶人表
CREATE EXTERNAL TABLE IF NOT EXISTS dwd.dwd_base_bd_drivinglicense(
  `csrq` string COMMENT '出生日期',
  `ljjf` string COMMENT '累积记分',
  `jyw` string COMMENT '校验位',
  `ly` string COMMENT '驾驶人来源',
  `syrq` string COMMENT '下一审验日期',
  `hmcd` string COMMENT '号码长度',
  `jyw2` string COMMENT '校验位2',
  `dzyx` string COMMENT '电子邮箱',
  `id` string COMMENT 'ID',
  `zjcx` string COMMENT '准驾车型',
  `qfrq` string COMMENT '下一清分日期',
  `fzjg` string COMMENT '发证机关',
  `xzqh` string COMMENT '行政区划',
  `lxdh` string COMMENT '联系电话',
  `fzrq` string COMMENT '发行驶证日期',
  `xzqj` string COMMENT '乡镇区局',
  `ydabh` string COMMENT '原档案编号',
  `sjhm` string COMMENT '手机号码',
  `dabh` string COMMENT '档案编号',
  `yxqz` string COMMENT '有效期止',
  `dwbh` string COMMENT '点位编号',
  `cclzrq` string COMMENT '初次领证日期',
  `gxsj` string COMMENT '更新时间',
  `yxqs` string COMMENT '有效期始',
  `sltj` string COMMENT 'sltj',
  `zt` string COMMENT '驾驶证状态',
  `jbr` string COMMENT '经办人',
  `zxbh` string COMMENT '证芯编号',
  `glbm` string COMMENT '管理部门',
  `lsh` string COMMENT '注册流水号',
  `sxqksbj` string COMMENT 'sxqksbj',
  `jzqx` string COMMENT '驾证期限',
  `zzfzrq` string COMMENT '暂住发证日期',
  `zzfzjg` string COMMENT '暂住发证机关',
  `zzzm` string COMMENT '暂住证明',
  `cfrq` string COMMENT '超分日期',
  `sfzmmc` string COMMENT '身份证明名称',
  `sqdm` string COMMENT '社区代码',
  `xxtzrq` string COMMENT '学习通知日期',
  `djzsxxdz` string COMMENT '登记住所详细地址',
  `jxmc` string COMMENT '驾校名称',
  `lxzsyzbm` string COMMENT '联系住所邮政编码',
  `bz` string COMMENT '备注',
  `sfzmhm` string COMMENT '身份证明号码',
  `xgzl` string COMMENT '相关资料',
  `sxbj` string COMMENT 'sxbj',
  `djzsxzqh` string COMMENT '登记住所行政区划',
  `sfbd` string COMMENT '是否本地',
  `xczg` string COMMENT 'xczg',
  `rnum` string COMMENT 'rnum',
  `xczjcx` string COMMENT 'xczjcx',
  `ccfzjg` string COMMENT '初次发证机关',
  `gj` string COMMENT '国籍',
  `bzcs` string COMMENT '补领行驶证次数',
  `xb` string COMMENT '性别',
  `xh` string COMMENT '序号',
  `syyxqz` string COMMENT 'syyxqz',
  `xzcrq` string COMMENT 'xzcrq',
  `ryzt` string COMMENT '人员状态',
  `xm` string COMMENT '姓名',
  `jly` string COMMENT '教练员',
  `lxzsxzqh` string COMMENT '联系住所行政区划',
  `yzjcx` string COMMENT '原准驾车型',
  `lxzsxxdz` string COMMENT '联系住所详细地址')
PARTITIONED BY
(
 ds   STRING
)
ROW FORMAT DELIMITED FIELDS TERMINATED BY '^'
STORED AS textfile
location '/daas/motl/dwd/dwd_base_bd_drivinglicense/';

-- 添加分区
alter table dwd.dwd_base_bd_drivinglicense add PARTITION(ds="20230425");

-- 将数据存入到hdfs中 hdfs dfs -put dwd_base_bd_drivinglicense.log /daas/motl/dwd/dwd_base_bd_drivinglicense/ds=20230425

-- 检验数据 select * from dwd.dwd_base_bd_drivinglicense limit 10;