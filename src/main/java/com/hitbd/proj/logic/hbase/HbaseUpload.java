package com.hitbd.proj.logic.hbase;

import com.hitbd.proj.util.Utils;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.*;

public class HbaseUpload {
    public static void main(String args[]) {
        // modified 1
        /*
        if (args.length < 1) {
            System.out.println("usage: trafficBD filename");
            return;
        }
        */
        String fileName = "/home/yang/IdeaProjects/trafficBD/src/data/alarm_8.txt";

        Configuration config = HBaseConfiguration.create();
        // modified 2
        // config.set("hbase.master", "192.168.31.140");
        // config.set("hbase.cluster.distributed", "false");

        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        HashMap<String, List<Put>> putMap = new HashMap<>();
        try (Connection connection = ConnectionFactory.createConnection(config)){
            System.out.println("Connect Success");

            // modified 3
            // CSVParser parser = new CSVParser(new FileReader(args[0]), CSVFormat.DEFAULT);

            CSVParser parser = new CSVParser(new FileReader(fileName), CSVFormat.DEFAULT);
            Iterator<CSVRecord> records = parser.iterator();
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
            records.next();
            Random random = new Random();
            while (records.hasNext()){
                CSVRecord record = records.next();
                // 获取Put列表
                Date createDate = sdf.parse(record.get(2));
                String tableName = Utils.getTableName(createDate);
                List<Put> putList;
                if (putMap.containsKey(tableName)) {
                    putList = putMap.get(tableName);
                }else{
                    putList = new ArrayList<>();
                    putMap.put(tableName, putList);
                }

                // 获取RowKey
                String imei = record.get(5);

                StringBuilder sb = new StringBuilder();
                for (int j = 0; j < 17 - imei.length(); j++) {
                    sb.append(0);
                }
                sb.append(imei).append(Utils.getRelativeSecond(createDate)).append(random.nextInt(10));
                String rowKey = sb.toString();

                // 获取Record
                sb.setLength(0);
                sb.append('\"').append(record.get(0)).append("\",").append(record.get(3)).append(',').append(record.get(4)).append(',');
                sb.append(record.get(6)).append(',').append(record.get(7)).append(',').append(record.get(8)).append(',').append(record.get(10));
                String rowRecord = sb.toString();

                // 放入批处理中
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn("r".getBytes(), "record".getBytes(), rowRecord.getBytes());
                put.addColumn("r".getBytes(), "stat".getBytes(), record.get(11).getBytes());
                put.addColumn("r".getBytes(), "type".getBytes(), record.get(1).getBytes());
                put.addColumn("r".getBytes(), "viewed".getBytes(), record.get(9).getBytes());
                putList.add(put);

                // 判断是否批上传
                if (putList.size() > 10000) {
                    System.out.println("正在上传10000条");
                    Table table = connection.getTable(TableName.valueOf(tableName));
                    table.put(putList);
                    table.close();
                    System.out.println("已上传10000条");
                    putMap.put(tableName, new ArrayList<>());
                }
            }
            // 上传最后未成批的部分
            for (Map.Entry<String, List<Put>> entry: putMap.entrySet()) {
                String tableName = entry.getKey();
                List<Put> putList = entry.getValue();
                Table table = connection.getTable(TableName.valueOf(tableName));
                table.put(putList);
                table.close();
            }
        }catch (Exception e) {
            e.printStackTrace();
        }
    }
    
}
