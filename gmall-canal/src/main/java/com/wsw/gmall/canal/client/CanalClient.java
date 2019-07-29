package com.wsw.gmall.canal.client;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.InvalidProtocolBufferException;
import com.wsw.gmall.canal.handler.CanalHandler;

import java.net.InetSocketAddress;
import java.util.List;

/**
 * @program: gmall-parent
 * @description:
 * @author: Mr.Wang
 * @create: 2019-07-23 11:11
 **/
public class CanalClient {

    public static void main(String[] args) {

        //创建连接器
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(new InetSocketAddress("hadoop102", 11111), "example", "", "");
        //实时监控的，抓取应该一直抓取
        while (true) {
            //创建与canal-server的连接
            canalConnector.connect();
            //抓取的表
            canalConnector.subscribe("gmall0218.*");
            //一次抓取的大小为100-entry的数量
            Message message = canalConnector.get(100);
            if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一会!");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }else{
                for (CanalEntry.Entry entry : message.getEntries()) {
                    //每一个entry对应一个SQL
                    //过滤一下entry，因为不是每个SQL都是对数据进行修改的写操作，比如，开关事务
                    if (entry.getEntryType().equals(CanalEntry.EntryType.ROWDATA)) {
                        CanalEntry.RowChange rowChange = null;
                        try {
                            //把StoreValue进行反序列化，得到rowChange
                            rowChange = CanalEntry.RowChange.parseFrom(entry.getStoreValue());
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();//得到行集
                        CanalEntry.EventType eventType = rowChange.getEventType();//得到事件类型，insert update delete drop alter
                        String tableName = entry.getHeader().getTableName();//表名
                        CanalHandler canalHandler = new CanalHandler(tableName,eventType,rowDatasList);
                        canalHandler.handle();

                    }
                }
            }

        }
    }
}
