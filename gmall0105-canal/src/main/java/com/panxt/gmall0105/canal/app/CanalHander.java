package com.panxt.gmall0105.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.panxt.gmall.constant.GmallConstant;
import com.panxt.gmall0105.canal.util.MyKafkaSender;
import org.apache.kafka.common.internals.Topic;

import javax.management.monitor.CounterMonitor;
import javax.swing.event.TableColumnModelEvent;
import java.util.List;

/**
 * 负责操作
 *
 * @author panxt
 * @create 2019-06-28 16:49
 */
public class CanalHander {

    List<CanalEntry.RowData> rowDatasList;
    String tableName;
    CanalEntry.EventType eventType;

    /**
     * 初始化
     * @param rowDatasList
     * @param tableName
     * @param eventType
     */
    public CanalHander(List<CanalEntry.RowData> rowDatasList, String tableName, CanalEntry.EventType eventType) {
        this.rowDatasList = rowDatasList;
        this.tableName = tableName;
        this.eventType = eventType;
    }

    /**
     *
     */
    public void handle(){
        if(eventType.equals(CanalEntry.EventType.INSERT)&&tableName.equals("order_info")){
            sendRowList2Kafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }else
        System.out.println("handle...");
    }

    /**
     * 将用canal从hbase中读到的数据发送到kafka
     * @param kafkaTopic
     */
    private void sendRowList2Kafka(String kafkaTopic){
        for (CanalEntry.RowData rowData : rowDatasList) {
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {

                System.out.println(column.getName() + "-->" + column.getValue());//输出到控制台测试
                jsonObject.put(column.getName(),column.getValue());
            }


            MyKafkaSender.send(kafkaTopic,jsonObject.toJSONString());
        }
    }


}
