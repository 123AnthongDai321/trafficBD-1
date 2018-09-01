package com.hitbd.proj.logic.hbase;

import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.IgniteSearch;
import com.hitbd.proj.model.Alarm;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.model.UserB;
import com.hitbd.proj.util.Serialization;
import com.hitbd.proj.util.Utils;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;


public class AlarmSearchUtils {

    /**
     * 根据userB id获取直属设备
     * @param userBId
     * @return
     */
    public HashMap<Integer, Long> getdirectDevicesOfUserB(Integer userBId){
        HashMap<Integer, Long> directDevicesOfUserB = new HashMap<Integer, Long>();
        IgniteSearch igniteSearchObj = new IgniteSearch();
        try{
            directDevicesOfUserB.put(userBId, igniteSearchObj.getDirectDevice(userBId).getImei());
        }catch (NotExistException e){
            e.printStackTrace();
        }
        return directDevicesOfUserB;
    }

    /**
     * 根据userB  id获取其能够访问的设备编号
     * @param userBId
     * @return
     */
    public HashMap<Integer, ArrayList<Long>> getImeiOfDevicesOfUserB(Integer userBId){
        HashMap<Integer, ArrayList<Long>> imeiOfDevicesOfUserB = new HashMap<Integer, ArrayList<Long>>();
        IgniteSearch igniteSearchObj = new IgniteSearch();
        try {
            UserB b = (UserB) igniteSearchObj.getUserB(userBId);

            ArrayList<Long> imeiOfDevicesOfB = new ArrayList<Long>();
            Serialization str2List = new Serialization();

            Queue<UserB> queue = new LinkedList<UserB>();
            queue.offer(b);
            while (!queue.isEmpty()) {
                UserB queueHead = queue.poll();
                imeiOfDevicesOfB.add(igniteSearchObj.getDirectDevice(queueHead.getUserBId()).getImei());
                ArrayList<Integer> children_list = str2List.strToList(queueHead.children_ids.toString());
                for (Integer element : children_list) {
                    queue.offer((UserB) igniteSearchObj.getUserB(element));
                }
            }
            imeiOfDevicesOfUserB.put(userBId, imeiOfDevicesOfB);
        } catch (NotExistException e) {
            e.printStackTrace();
        }
        return imeiOfDevicesOfUserB;
    }

    /**
     * 根据imei与创建时间与E创建行键rouKey
     *
     */
    public String createRowKey(Long imei, Date createTime, int E){
        String imeiString = String.valueOf(imei);
        StringBuilder sb = new StringBuilder();

        for (int j = 0; j < 17 - imeiString.length(); j++) {
            sb.append(0);
        }
        String rowKey = sb.append(imeiString).append(Utils.getRelativeSecond(createTime)).append(String.valueOf(E)).toString();
        return rowKey;
    }

    public HashMap<Long, Pair<String, String>> createRowkeyForQueryByImei(List<Long> imeis){
        HashMap<Long, Pair<String, String>> imeisRowKey = new HashMap<Long, Pair<String, String>>();
        for(Long element: imeis){
            StringBuilder sb = new StringBuilder();
            String imeiStr = String.valueOf(element);
            for (int j = 0; j < 17 - imeiStr.length(); j++) {
                sb.append(0);
            }
            sb.append(element).append("00000").append("0");
            String startRowKey = sb.toString();

            sb.setLength(0);
            imeiStr = String.valueOf(element);
            for (int j = 0; j < 17 - imeiStr.length(); j++) {
                sb.append(0);
            }
            sb.append(imeiStr).append("fffff").append("9");
            String endRowKey = sb.toString();
            Pair<String, String> pair = new Pair<>(startRowKey, endRowKey);
            imeisRowKey.put(element, pair);
        }
        return imeisRowKey;
    }

    public Iterator<Result> scanTable(String tableName, String start, String end, Connection connection) throws IOException {
        Table table = connection.getTable(TableName.valueOf(tableName));  //IOException
        Scan scan = new Scan(start.getBytes(),end.getBytes());
        ResultScanner scanner = table.getScanner(scan);
        Iterator<Result> ret = scanner.iterator();
        scanner.close();
        table.close();
        return ret;
    }
    public void addToList(Iterator<Result> results, List<IAlarm> ret, long basicTime) throws ParseException {
        while(results.hasNext()) {
            Result r = results.next();
            String rowKey = Bytes.toString(r.getRow());
            IAlarm alarm = new Alarm();
            alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
            Date createDate = new Date(Long.valueOf(rowKey.substring(17,22),16)*1000+basicTime);
            alarm.setCreateTime(createDate);
            alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
            alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
            alarm.setViewed(Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0")==true?false:true);
            String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
            String[] recordArr = record.split(",");
            alarm.setAddress(recordArr[0]);
            alarm.setEncId(recordArr[1]);
            alarm.setId(recordArr[2]);
            alarm.setLatitude(Float.valueOf(recordArr[3]));
            alarm.setLongitude(Float.valueOf(recordArr[4]));
            SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            alarm.setPushTime(dateformatter.parse(recordArr[5]));  //parseException
            alarm.setVelocity(Float.valueOf(recordArr[6]));
            ret.add(alarm);
            }
    }

    public void addToList(Iterator<Result> results, Date startTime, Date endTime, List<IAlarm> ret, long basicTime) throws ParseException {
        while(results.hasNext()) {
            Result r = results.next();
            String rowKey = Bytes.toString(r.getRow());

            Date createDate = new Date(Long.valueOf(rowKey.substring(17,22),16)*1000+basicTime);

            if(createDate.after(startTime)&&createDate.before(endTime)) {
                IAlarm alarm = new Alarm();
                alarm.setImei(Long.valueOf(rowKey.substring(0, 17)));
                alarm.setCreateTime(createDate);
                alarm.setStatus(Bytes.toString(r.getValue("r".getBytes(), "stat".getBytes())));
                alarm.setType(Bytes.toString(r.getValue("r".getBytes(), "type".getBytes())));
                alarm.setViewed(Bytes.toString(r.getValue("r".getBytes(), "viewed".getBytes())).equals("0")==true?false:true);
                String record = Bytes.toString(r.getValue("r".getBytes(), "record".getBytes()));
                String[] recordArr = record.split(",");
                alarm.setAddress(recordArr[0]);
                alarm.setEncId(recordArr[1]);
                alarm.setId(recordArr[2]);
                alarm.setLatitude(Float.valueOf(recordArr[3]));
                alarm.setLongitude(Float.valueOf(recordArr[4]));
                SimpleDateFormat dateformatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                alarm.setPushTime(dateformatter.parse(recordArr[5]));  //parseException
                alarm.setVelocity(Float.valueOf(recordArr[6]));
                ret.add(alarm);
            }
        }
    }


}
