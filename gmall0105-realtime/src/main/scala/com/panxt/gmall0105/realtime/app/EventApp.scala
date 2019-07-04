package com.panxt.gmall0105.realtime.app

import java.util

import com.alibaba.fastjson.JSON
import com.panxt.gmall.constant.GmallConstant
import com.panxt.gmall0105.realtime.bean.{CouponAlertInfo, EventInfo}
import com.panxt.gmall0105.realtime.util.{MyEsUtil, MyKafkaUtil}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.streaming.{Seconds, StreamingContext}

import scala.util.control.Breaks._

/**
  * @author panxt
  */
object EventApp {

  def main(args: Array[String]): Unit = {

    // TODO 日志预警
    // 需求：同一设备，5分钟内三次及以上用不同账号登录并领取优惠劵，并且在登录到领劵过程中没有浏览商品。产生一条预警日志。(同一设备每分钟只记录一次预警)
    //      日志格式：
    //      mid  设备id
    //      uids 领取优惠券登录过的uid
    //      itemIds 优惠券涉及的商品id
    //      events  发生过的行为
    //      ts 发生预警的时间戳

    val conf: SparkConf = new SparkConf().setMaster("local[*]").setAppName("EventApp")
    //    val context: StreamingContext = new StreamingContext(conf,Seconds(5))
    val ssc: StreamingContext = new StreamingContext(new SparkContext(conf), Seconds(5))

    val inputDStream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream(GmallConstant.KAFKA_TOPIC_EVENT, ssc)

    //连接测试 success
    //{"area":"beijing","uid":"286","itemid":5,"npgid":3,
    // "evid":"addCart","os":"ios","pgid":15,"appid":"gmall0105","mid":"mid_286","type":"event","ts":1561796624932}
    /*inputDStream.map(_.value()).foreachRDD(rdd =>
      println(rdd.collect.mkString("\n"))
    )*/

    //0.用样例类来转换方便操作 EventInfo
    val eventInfoStream: DStream[EventInfo] = inputDStream.map { record =>
      val eventInfo = JSON.parseObject(record.value(), classOf[EventInfo])
      eventInfo
    }

    //开窗（实验模拟，为了表现效果，周期设置较短）
    val eventInfoWindowDStream: DStream[EventInfo] = eventInfoStream.window(Seconds(30), Seconds(5))

    //1.同一设备进行分组
    val groupDStream: DStream[(String, Iterable[EventInfo])] = eventInfoWindowDStream.map(x => (x.mid, x)).groupByKey()

    //2.判断预警
    //    在一个设备之内
    //    1 三次及以上的领取优惠券 (evid coupon) 且 uid都不相同
    //    2 没有浏览商品(evid  clickItem)
    //    需要的信息：（mid,uidset,itemidSet,eventset,timestamp）
    //    uidset:筛选3次以上
    //    itemset:涉及商品的id，指标数据分析时可能要用
    //    ventset:事件集合，用于过滤
    //    timestamp:锚点，用于各种时间上的处理
    val checkCouponAlertDStream: DStream[(Boolean, CouponAlertInfo)] = groupDStream.map { case (mid, eventIter) =>
      val eventIds = new util.ArrayList[String]()
      val couponUidSets = new util.HashSet[String]()
      val couponItemSets = new util.HashSet[String]()
      var notClickItem: Boolean = true

      breakable(
        eventIter.foreach { eventinfo =>
          eventIds.add(eventinfo.evid) //用户行为
          if (eventinfo.evid == "coupon") {
            couponUidSets.add(eventinfo.uid) //用户领卷的uid
            couponItemSets.add(eventinfo.itemid) //用户领卷的商品id
          } else if (eventinfo.evid == "clickItem") {
            notClickItem = false
            break()
          }

        }
      )
      //组合成元祖  （标识是否达到预警要求，预警信息对象）
      (couponItemSets.size() >= 3 && notClickItem, CouponAlertInfo(mid, couponUidSets, couponItemSets, eventIds, System.currentTimeMillis()))

    }

    /*checkCouponAlertDStream.foreachRDD(rdd=> //success
      println(rdd.collect.mkString("\n"))
    )*/

    //简单过滤

    val filterDStram: DStream[(Boolean, CouponAlertInfo)] = checkCouponAlertDStream.filter(_._1)

    /*filterDStram.foreachRDD(rdd=>
      println(rdd.collect.mkString("\n"))
    )*/
    //增加一个字段，用于保存到es中进行去重
    val alertInfoWithIdDstream: DStream[(String, CouponAlertInfo)] = filterDStram.map { case (flag, event) =>

      val period: Long = event.ts / 1000L / 60L //分钟为单位
    val id: String = event.mid + "_" + period
      (id, event)
    }

    /*alertInfoWithIdDstream.foreachRDD(rdd=> //success
      println(rdd.collect.mkString("\n"))
    )*/


    //批量存储到es中
    alertInfoWithIdDstream.foreachRDD { rdd =>
      rdd.foreachPartition { alertInfoWithIdIter =>
        MyEsUtil.insertBulk(GmallConstant.ES_INDEX_COUPON_ALTER, alertInfoWithIdIter.toList)
      }
    }


    ssc.start()
    ssc.awaitTermination()

  }

}
