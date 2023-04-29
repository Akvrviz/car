package com.zsj.stream

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.assigners.{SlidingEventTimeWindows, SlidingProcessingTimeWindows, TumblingEventTimeWindows}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.sql.PreparedStatement
import java.text.SimpleDateFormat
import java.time.Duration

object CardAvgFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    env.setParallelism(1)

    /**
     * 将kafka作为数据源
     */
    val source: KafkaSource[String] = KafkaSource
      .builder[String]
      .setBootstrapServers("master:9092,node1:9092,node2:9092") // kafka 集群的列表
      .setTopics("cars") //消费的topic
      .setGroupId("my-group1") //消费者组
      .setStartingOffsets(OffsetsInitializer.earliest()) //指定读取数据的位置
      .setValueOnlyDeserializer(new SimpleStringSchema()) //指定反序列化数据的类
      .build

    val kafkaDS: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")

    val cardTimeSpeedDS: DataStream[(String, Long, Double)] = kafkaDS
      .filter(car =>{
        try {
          var carJson: JSONObject = JSON.parseObject(car)
          if (carJson == null) {
            false
          }
          true

        } catch {
          case e: RuntimeException =>
            false
        }
      })
      .map(car => {
        val carJson: JSONObject = JSON.parseObject(car)
        val card: String = carJson.getString("card")
        val time: Long = carJson.getLong("time")
        val speed: Double = carJson.getDouble("speed")

        (card, time*1000L, speed)
      })



    val resultDS: DataStream[(String, Long, Long, Double, Long)] = cardTimeSpeedDS
        .assignTimestampsAndWatermarks(
        WatermarkStrategy
          // 将水位线前移到到来数据最大时间戳的前5秒,即如果该时间戳最大为x,则水位线为x-5
          // 含义：乱序到达的误差可达 5 秒
          // 默认水位线为所有记录（数据）中时间戳最大值
          .forBoundedOutOfOrderness[(String, Long, Double)](Duration.ofSeconds(10))
          //指定该记录的时间戳
          .withTimestampAssigner(
            new SerializableTimestampAssigner[(String, Long, Double)] {
              override def extractTimestamp(element: (String, Long, Double), recordTimestamp: Long): Long = {
                //时间字段,单位毫秒
                element._2
              }
            }
          )
        )
      .keyBy(_._1)
      .window(SlidingEventTimeWindows.of(Time.minutes(5), Time.minutes(1)))
//      .window(TumblingEventTimeWindows.of(Time.seconds(2)))
      .process(new MyProcessWindowFunction())



    /**
     * 构建jdbcSink
     */
    val jdbcSink: SinkFunction[(String, Long, Long, Double, Long)] = JdbcSink.sink(
      // 插入数据的sql
      "replace into card_avg_flow (card, start_time, end_time, avg_speed, count) values(?,?,?,?,?)",
      new JdbcStatementBuilder[(String, Long, Long, Double, Long)] {
        override def accept(state: PreparedStatement, value: (String, Long, Long, Double, Long)): Unit = {
          val card: String = value._1
          val startTime: Long = value._2
          val endTime: Long = value._3
          val speed: Double = value._4
          val count: Long = value._5

          val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

          state.setString(1, card)
          state.setString(2, simpleDateFormat.format(startTime))
          state.setString(3, simpleDateFormat.format(endTime))
          state.setDouble(4, speed)
          state.setLong(5, count)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://master:3306/traffic?useSSL=false&useUnicode=true&characterEncoding=UTF-8")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root")
        .withPassword("123456")
        .build()
    )

    resultDS.addSink(jdbcSink)

    env.execute()
  }
}
class MyProcessWindowFunction extends ProcessWindowFunction[(String, Long, Double),(String, Long, Long, Double, Long), String, TimeWindow]{
  override def process(key: String,
                       context: Context,
                       elements: Iterable[(String, Long, Double)],
                       out: Collector[(String, Long, Long, Double, Long)]): Unit = {

    // 窗口中所有数据的总车速
    var totalSpeed : Double = 0

    // 窗口中的中车数
    var count : Long = 0L

    // 循环计算平均车速
    for (elem <- elements) {
      totalSpeed += elem._3
      count += 1
    }
    var avgSpeed : Double = totalSpeed / count

    // 获取窗口的始末时间
    val startTime: Long = context.window.getStart
    val endTime: Long = context.window.getEnd

    // 将数据输出到下游
    out.collect((key, startTime, endTime, avgSpeed, count))
  }
}
