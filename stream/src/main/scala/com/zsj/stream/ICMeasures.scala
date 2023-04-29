package com.zsj.stream

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.connector.jdbc.JdbcInputFormat
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, Table, TableEnvironment}
import org.apache.flink.types.Row

/**
 * 缉查布控
 */
object ICMeasures {
  def main(args: Array[String]): Unit = {
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .inStreamingMode() //流处理模式
      .build()

    val tableEnv: TableEnvironment = TableEnvironment.create(settings)

    /**
     * 从Kafka中获取车流数据
     */
    tableEnv.executeSql(
      """
        |CREATE TABLE cars (
        |  `car` STRING,
        |  `city_code` STRING,
        |  `county_code` STRING,
        |  `card` BIGINT,
        |  `camera_id` STRING,
        |  `orientation` STRING,
        |  `road_id` BIGINT,
        |  `time` BIGINT,
        |  `speed`  DOUBLE
        |) WITH (
        |  'connector' = 'kafka',
        |  'topic' = 'cars',
        |  'properties.bootstrap.servers' = 'master:9092,nod1:9092,node2:9092',
        |  'properties.group.id' = 'ICMeasures',
        |  'scan.startup.mode' = 'earliest-offset',
        |  'format' = 'json',
        |  'json.ignore-parse-errors' = 'true'
        |)
        |
      """.stripMargin)

    /**
     * 从mysql中获取需要缉查的车辆牌照
     */
    tableEnv.executeSql(
      """
        |CREATE TABLE control_list (
        |  car STRING,
        |  PRIMARY KEY (car) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://master:3306/traffic?useSSL=false&useUnicode=true&characterEncoding=UTF-8',
        |   'table-name' = 'control_list',
        |   'username' = 'root',
        |   'password' = '123456'
        |);
        |""".stripMargin)


    tableEnv.executeSql(
      """
        |CREATE TABLE control_cars (
        |  `car` STRING,
        |  `city_code` STRING,
        |  `county_code` STRING,
        |  `card` BIGINT,
        |  `camera_id` STRING,
        |  `orientation` STRING,
        |  `road_id` BIGINT,
        |  `time` BIGINT,
        |  `speed`  DOUBLE,
        |  PRIMARY KEY (`car`,`time`) NOT ENFORCED
        |) WITH (
        |   'connector' = 'jdbc',
        |   'url' = 'jdbc:mysql://master:3306/traffic?useSSL=false&useUnicode=true&characterEncoding=UTF-8',
        |   'table-name' = 'control_cars',
        |   'username' = 'root',
        |   'password' = '123456'
        |)
      """.stripMargin)

    tableEnv.executeSql(
      """
        |INSERT INTO control_cars
        |SELECT
        |cars.*
        |FROM cars
        |INNER JOIN control_list
        |ON cars.car = control_list.car
        |""".stripMargin)

  }
}
