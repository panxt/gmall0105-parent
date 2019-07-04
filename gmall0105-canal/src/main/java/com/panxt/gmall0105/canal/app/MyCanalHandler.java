package com.panxt.gmall0105.canal.app;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.panxt.gmall.constant.GmallConstant;
import com.panxt.gmall0105.canal.util.MyKafkaSender;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.util.List;

import static com.panxt.gmall0105.canal.util.MyKafkaSender.kafkaProducer;

/**
 * @author panxt
 * @create 2019-07-02 18:57
 */
public class MyCanalHandler {

    List<CanalEntry.RowData> rowDatasList;
    String tableName;
    CanalEntry.EventType eventType;

    public MyCanalHandler(List<CanalEntry.RowData> rowDatasList, String tableName, CanalEntry.EventType eventType) {
        this.rowDatasList = rowDatasList;
        this.tableName = tableName;
        this.eventType = eventType;
    }

    public void handle() {
        if (eventType.equals(CanalEntry.EventType.INSERT) && tableName.equals("order_info")) {

            sendRowList2Kafka(GmallConstant.KAFKA_TOPIC_ORDER);
        }

    }

    private void sendRowList2Kafka(String kafkaTopic) {

        for (CanalEntry.RowData rowData : rowDatasList) {
            int i = 5;//废话
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
                System.out.println(column.getName() + "-->" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }

            MyKafkaSender.send(kafkaTopic, jsonObject.toJSONString());
        }

    }
}
