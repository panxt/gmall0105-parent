package com.panxt.gmall0105.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @author panxt
 * @create 2019-07-02 16:39
 */
public class MyCanalClient {

    public static void watch (String hostname,int port,String destination,String tables){

        //构建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port),
                destination, "", "");

        //一直去访问mysql的binlog,获取信息
        while(true){
            canalConnector.connect();
            canalConnector.subscribe(tables);
            Message message = canalConnector.get(100);//一次最多取出多少条sql

            int size = message.getEntries().size();

            if(size == 0){
                System.err.println("no datas, sleep a while!");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                for (CanalEntry.Entry entry : message.getEntries()) { //每一条entry代表一条sql
                    //有可能是执行语句，也可能是执行事务的语句,只要执行语句
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){

                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {//ProtocolBuffer:Google出的一种序列化方式，数度快
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集
                        String tableName = entry.getHeader().getTableName();//表名
                        CanalEntry.EventType eventType = rowChange.getEventType();//操作行为 insert update delete

                        MyCanalHandler myCanalHandler = new MyCanalHandler(rowDatasList, tableName, eventType);
                        myCanalHandler.handle();
                    }
                }
            }


        }


    }

}
