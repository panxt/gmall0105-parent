package com.panxt.gmall0105.realtime.app

import com.alibaba.fastjson.JSON
import com.panxt.gmall.constant.GmallConstant
import com.panxt.gmall0105.realtime.bean.OrderInfo
import com.panxt.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * @author panxt
  *
  */
object MyOrderAPP {

  def main(args: Array[String]): Unit = {

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("MyOrderAPP")
    val ssc: StreamingContext = new StreamingContext(conf, Seconds(5))

    // 从mysql中用canal取出的user_info 数据 通过kafka发送到hbase中去
    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_ORDER, ssc)
    val mapDstream: DStream[OrderInfo] = inputDStream.map { x =>
      val jedis = RedisUtil.getJedisClient

      val info = JSON.parseObject(x.value(), classOf[OrderInfo])

      //TODO 识别是否首次下单
      if(jedis.smembers("first_order_userId").contains(info.user_id)!=null){
        info.first_order = false
      }else{
        info.first_order = true
        jedis.sadd("first_order_userId",info.user_id)
      }
      //补充日期
      val dataTime = info.create_time.split(" ")
      info.create_date = dataTime(0)
      info.create_hour = dataTime(1).split(":")(0)
      //电话脱敏
      info.consignee_tel = "*******" + info.consignee_tel.splitAt(7)._2

      jedis.close()
      info
    }

    //保存到hbase ==>TODO hbase建表
    import org.apache.phoenix.spark._
    mapDstream.foreachRDD(rdd=>
      rdd.saveToPhoenix("my_gmall0105_order_info",
        Seq("ID","PROVINCE_ID", "CONSIGNEE", "ORDER_COMMENT", "CONSIGNEE_TEL",
          "ORDER_STATUS", "PAYMENT_WAY", "USER_ID","IMG_URL", "TOTAL_AMOUNT",
          "EXPIRE_TIME", "DELIVERY_ADDRESS", "CREATE_TIME","OPERATE_TIME","TRACKING_NO",
          "PARENT_ORDER_ID","OUT_TRADE_NO", "TRADE_BODY", "CREATE_DATE", "CREATE_HOUR","FIRST_ORDER"),
        new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    )

    ssc.start()
    ssc.awaitTermination()


  }

}
