package com.panxt.gmall0105.canal.app;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * 负责连接
 *
 * @author panxt
 * @create 2019-06-28 16:20
 */
public class CanalClient {

    public static void watch(String hostname,int port,String destination,String tables){
        //构建连接器

        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress(hostname, port), destination, "", "");


        while(true){
            canalConnector.connect();
            canalConnector.subscribe(tables);
            Message message = canalConnector.get(100);

            int size = message.getEntries().size();

            if(size == 0){
                //没数据，睡眠5s等待下一次读取。
                System.out.println("no dates,sleep 5s!");

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else {
                //有数据，进行操作。
                for (CanalEntry.Entry entry : message.getEntries()) {

                    //判断操作类型，过滤事件操作
                    if(entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)){
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //entry.getStoreValue() : 返回值为ByteString（Google的一种编码格式）,用特定方法去解析。
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//行集
                        String tableName = entry.getHeader().getTableName();//表名
                        CanalEntry.EventType eventType = rowChange.getEventType();//操作行为 insert,update,delete
                        CanalHander canalHander = new CanalHander(rowDatasList, tableName, eventType);//初始化数据
                        canalHander.handle();//执行写操作
                    }


                }


            }

        }

    }



}
