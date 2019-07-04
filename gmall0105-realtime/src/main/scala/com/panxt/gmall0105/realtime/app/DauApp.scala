package com.panxt.gmall0105.realtime.app

import java.util
import java.util.Date

import com.alibaba.fastjson.JSON
import com.panxt.gmall.constant.GmallConstant
import com.panxt.gmall.util.DateUtil
import com.panxt.gmall0105.realtime.bean.StartUpLog
import com.panxt.gmall0105.realtime.util.{MyKafkaUtil, RedisUtil}
import org.apache.hadoop.conf.Configuration
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.phoenix.spark._

/**
  * @author panxt
  */
object DauApp {

  /**
    * 目的 ： 日活DAU
    * 注意点：①DAU核心就是去重，数据从kafka中消费数据，去重后写到Redis和Phoenix中，首先每个rdd去重，然后，全局去重。
    * ②注意控制开启连接的消耗。
    *
    * @param args
    */
  def main(args: Array[String]): Unit = {
    val sparkConf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("DauApp")
    val sc = new SparkContext(sparkConf)
    val ssc = new StreamingContext(sc, Seconds(5))

    ssc.sparkContext.setCheckpointDir("./checkpoint")

    val inputDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_STARTUP, ssc)

    // 测试连接通畅。ture
    //    inputDstream.map(_.value()).foreachRDD { rdd =>
    //      println(rdd.collect().mkString("\n"))
    //    }

    //    val startupLogDstream: DStream[StartUpLog] = inputDstream.map(_.value()).map { log =>
    //      // println(s"log = ${log}")
    //      val startUpLog: StartUpLog = JSON.parseObject(log, classOf[StartUpLog])
    //      startUpLog
    //    }

    // 1.数据流：将数据转变为case class 并补充两个时间字段
    val startLogDStream: DStream[StartUpLog] = inputDstream.map { record =>
      val jsonStr: String = record.value()
      val startUpLog: StartUpLog = JSON.parseObject(jsonStr, classOf[StartUpLog])

      startUpLog.logDate = DateUtil.getDate(startUpLog.ts)
      startUpLog.logHour = DateUtil.getDateHour(startUpLog.ts)

      startUpLog
    }

    startLogDStream.cache() //同时要用于Redis和hbase + phoenix,所以进行缓存，避免重复计算

    // 2. 用用户清单进行过滤，去重，至保留用户清单中
    val filterDStream: DStream[StartUpLog] = startLogDStream.transform { rdd =>
      val client = RedisUtil.getJedisClient
      val key = "dau:" + DateUtil.getDate(System.currentTimeMillis())
      val dauMidSet: util.Set[String] = client.smembers(key)
      client.close()

      val dauMidBC: Broadcast[util.Set[String]] = ssc.sparkContext.broadcast(dauMidSet)
      println("过滤前：" + rdd.count)
      val filterRDD: RDD[StartUpLog] = rdd.filter { startuplog =>
        val midSet: util.Set[String] = dauMidBC.value
        !midSet.contains(startuplog.mid)
      }
      println("过滤后：" + filterRDD.count)
      filterRDD
    }

    //3. 批次内进行去重，2对于第一批内有大量同一用户的极端情况无效 ==>success
    val distinctDStream: DStream[StartUpLog] = filterDStream.map(startuplog => (startuplog.mid, startuplog)).groupByKey().flatMap { case (mid, startuplog) => startuplog.toList.take(1) }

    //4. 保存今日访问过的用户名单 -->redis key:set ==> key => dau : date value : mid
    distinctDStream.foreachRDD{rdd=>
      rdd.foreachPartition { startuplogItr =>
        val client = RedisUtil.getJedisClient
//        startuplogItr.foreach{startuplog=>
        for(startuplog <- startuplogItr){
          val key: String = "dau:"+startuplog.logDate
          client.sadd(key,startuplog.mid)
          println(startuplog)
        }
        client.close()
      }

    }

    //5. 将数据写入hbase + phoenix
    import org.apache.phoenix.spark._ //必写
    distinctDStream.foreachRDD{rdd=>
      rdd.saveToPhoenix("GMALL0105_DAU",Seq("MID", "UID", "APPID", "AREA", "OS", "CH", "TYPE", "VS", "LOGDATE", "LOGHOUR", "TS") ,new Configuration,Some("hadoop102,hadoop103,hadoop104:2181"))
    }


    ssc.start()
    ssc.awaitTermination()
  }
}
