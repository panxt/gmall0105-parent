package com.panxt.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.panxt.gmall.constant.GmallConstant
import com.panxt.gmall.util.DateUtil
import com.panxt.gmall0105.realtime.bean.{OrderInfo, StartUpLog}
import com.panxt.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._
/**
  * @author panxt
  */
object OrderApp {

  /**
    * 目的 ： 日活DAU
    * 注意点：①DAU核心就是去重，数据从kafka中消费数据，去重后写到Redis和Phoenix中，首先每个rdd去重，然后，全局去重。
    * ②注意控制开启连接的消耗。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("OrderApp")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)


    val orderInfoDstream: DStream[OrderInfo] = inputDstream.map { record =>
      val orderInfo: OrderInfo = JSON.parseObject(record.value(), classOf[OrderInfo])

      val datetimeArr = orderInfo.create_time.split(" ")

      orderInfo.create_date = datetimeArr(0)
      val timeArr: Array[String] = datetimeArr(1).split(":")
      orderInfo.create_hour = timeArr(0)

      //电话脱敏
      val telTuple: (String, String) = orderInfo.consignee_tel.splitAt(7)
      orderInfo.consignee_tel = "*******" + telTuple._2

      orderInfo

      // 增加一个字段 标识是否是用户首次下单

    }


    //保存到hbase
    orderInfoDstream.foreachRDD{rdd=>

      rdd.saveToPhoenix("gmall0105_order_info",Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL", "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT", "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO","PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181") )

    }


    ssc.start()
    ssc.awaitTermination()
  }
}
