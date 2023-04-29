package com.zsj.stream

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.connector.jdbc.{JdbcConnectionOptions, JdbcSink, JdbcStatementBuilder}
import org.apache.flink.connector.kafka.source.KafkaSource
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig.ExternalizedCheckpointCleanup
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.scala._

import java.sql.PreparedStatement
import java.text.SimpleDateFormat

object CardDailyFlow {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment

    /**
     * 开启flink的快照
     */
    // 每 5000ms 开始一次 checkpoint
    env.enableCheckpointing(5000)

    // 高级选项：

    // 设置模式为精确一次 (这是默认值)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)

    // 确认 checkpoints 之间的时间会进行 500 ms
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(500)

    // Checkpoint 必须在一分钟内完成，否则就会被抛弃
    env.getCheckpointConfig.setCheckpointTimeout(60000)

    // 允许两个连续的 checkpoint 错误
    env.getCheckpointConfig.setTolerableCheckpointFailureNumber(2)

    // 同一时间只允许一个 checkpoint 进行
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    // 使用 externalized checkpoints，这样 checkpoint 在作业取消后仍就会被保留
    env.getCheckpointConfig.setExternalizedCheckpointCleanup(
      ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    /**
     * 设置checkpoint保存数据方式和位置
     * 两种状态后端：
     * rocksDB状态后端：先保存在本地数据库，等到checkpoint启动后再存入hdfs中，适合状态量大
     * HashMap状态后端：先保存到内存中，等到checkpoint启动后存入hdfs中，适合高性能高吞吐
     */
    //rocksDB状态后端，参数为true启动增量检查点
    env.setStateBackend(new EmbeddedRocksDBStateBackend())
    //env.setStateBackend(new HashMapStateBackend())
    env.getCheckpointConfig.setCheckpointStorage("hdfs://master:9000/flink/car/")


    /**
     * 将kafka作为数据源
     */
    val source: KafkaSource[String] = KafkaSource
      .builder[String]
      .setBootstrapServers("master:9092,node1:9092,node2:9092") // kafka 集群的列表
      .setTopics("car") //消费的topic
      .setGroupId("my-group1") //消费者组
      .setStartingOffsets(OffsetsInitializer.earliest()) //指定读取数据的位置
      .setValueOnlyDeserializer(new SimpleStringSchema()) //指定反序列化数据的类
      .build

    val kafkaDS: DataStream[String] = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")

    val cardDateDS: DataStream[(String, String, Long)] = kafkaDS
      .filter(JSON.parseObject(_) != null)
      .map(car => {
        val carJson: JSONObject = JSON.parseObject(car)
        val card: String = carJson.getString("card")
        val time: Long = carJson.getLong("time")

        // 将时间戳转换成日期格式
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val date: String = dateFormat.format(time * 1000L)

        (card, date, 1L)
      })

    val cardFlowCountDS: DataStream[String] = cardDateDS
      .keyBy(cd => (cd._1, cd._2))
      .sum(2)
      .map(t => s"${t._1},${t._2},${t._3}")

    val jdbcSink: SinkFunction[String] = JdbcSink.sink(
      // 插入数据的sql
      "replace into card_daily_flow (card, date, flow) values(?,?,?)",
      new JdbcStatementBuilder[String] {
        override def accept(state: PreparedStatement, value: String): Unit = {
          print(value + "\n")
          val split: Array[String] = value.split(",")
          val card: String = split(0)
          val date: String = split(1)
          val flow: Long = split(2).toLong

          state.setString(1, card)
          state.setString(2, date)
          state.setLong(3, flow)
        }
      },
      new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
        .withUrl("jdbc:mysql://master:3306/traffic?useSSL=false&useUnicode=true&characterEncoding=UTF-8")
        .withDriverName("com.mysql.jdbc.Driver")
        .withUsername("root")
        .withPassword("123456")
        .build()
    )

    cardFlowCountDS.addSink(jdbcSink)

    env.execute()
  }
}
