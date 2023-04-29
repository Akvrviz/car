package com.zsj

import org.apache.kafka.clients.producer.KafkaProducer

import java.util.Properties

object MakeCarDataToKafka {
  def main(args: Array[String]): Unit = {

    // 获取

    // 设置 Kafka 生产者的配置信息
    val props = new Properties()
    props.put("bootstrap.servers", "master:9092,node1:9092,node2:9092")
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")

    // 创建 Kafka Producer 实例
    val producer = new KafkaProducer[String, String](props)
  }
}
