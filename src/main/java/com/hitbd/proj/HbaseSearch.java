package com.hitbd.proj;

import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.logic.hbase.AlarmSearchUtils;
import com.hitbd.proj.model.IAlarm;
import com.hitbd.proj.model.Pair;
import com.hitbd.proj.util.Utils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Result;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.util.*;

public class HbaseSearch implements IHbaseSearch {

    static Configuration config = null;
    static Connection connection = null;

    public boolean connect() {
        // need to modify

        config.set("hbase.zookeeper.quorum", "localhost");
        config.set("hbase.zookeeper.property.clientPort", "2181");

        if (connection == null || connection.isClosed()) {
            try {
                Configuration config = HBaseConfiguration.create();
                connection = ConnectionFactory.createConnection(config);
            } catch (IOException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean connect(Configuration config) {
        return false;
    }

    @Override
    public List<IAlarm> getAlarms(long startImei, long endImei, Date startTime, Date endTime) {
        return null;
    }

    @Override
    public void insertAlarm(List<IAlarm> alarms) throws TimeException, ForeignKeyException {

    }

    @Override
    public void setPushTime(List<Pair<String, String>> rowKeys, Date pushTime) throws NotExistException {

    }

    @Override
    public void setViewedFlag(List<Pair<String, String>> rowKeys, boolean viewed) throws NotExistException {

    }

    @Override
    public void deleteAlarm(List<Pair<String, String>> rowKeys) throws NotExistException {

    }

    @Override
    public List<IAlarm> queryAlarmByUser(List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        IgniteSearch  igniteSearchObj = new IgniteSearch();

        // 用户id为user_id的所有子用户
        HashMap<Integer, ArrayList<Integer>> childrenOfUserB = new HashMap<Integer, ArrayList<Integer>>();
        HashMap<Integer, Long> directDevicesOfUserB = new HashMap<Integer, Long>();
        HashMap<Integer, ArrayList<Long>> imeiOfDevicesOfUserB = new HashMap<Integer, ArrayList<Long>>();

        // 创建使用AlarmSearchUtil的对象,方便操作
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        if(igniteSearchObj.connect()){
            for (Integer userBId : userBIds) {
                directDevicesOfUserB.putAll(utilsObj.getdirectDevicesOfUserB(userBId));
                imeiOfDevicesOfUserB.putAll(utilsObj.getImeiOfDevicesOfUserB(userBId));
            }
        }
        // 通过imeiOfDevicesOfUserB 查询用户的所有警告表

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        // 根据imei与创建时间与E创建行键rouKeys
        List<Pair<String, String>> rowKeys = new ArrayList<>();
        Date startTime = allowTimeRange.getKey();
        Date endTime = allowTimeRange.getValue();

        Random random = new Random();
        for(Long element: allowIMEIs) {
            String startRowKey = utilsObj.createRowKey(element, startTime, random.nextInt(10));
            String endRowKey = utilsObj.createRowKey(element, endTime, random.nextInt(10));
            Pair<String, String> pair = new Pair<>(startRowKey, endRowKey);
            rowKeys.add(pair);
        }

        // 警告表查询结果
        List<IAlarm> queryResult = new ArrayList<>();

        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
        Date date48before = new Date(Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4 - 48 * 1000L * 60 * 60 * 24);

        if(endTime.after(date48before)) {
            Date mmddstartTime = startTime.before(date48before) == true ? date48before : startTime;
            String endTableName = Utils.getTableName(endTime);

            while(!Utils.getTableName(mmddstartTime).equals(endTableName)) {
                for(Pair<String, String> pair: rowKeys){
                    try{
                        Iterator<Result> results = utilsObj.scanTable(Utils.getTableName(mmddstartTime),pair.getKey(),pair.getValue(), connection);
                        milliSecond = mmddstartTime.getTime() - Settings.BASETIME;
                        period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
                        utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                        mmddstartTime = new Date(mmddstartTime.getTime()+ 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException e){
                        e.printStackTrace();
                    }catch (ParseException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        if(startTime.before(date48before)){
            for(Pair<String, String> pair: rowKeys){
                try{
                    Iterator<Result> results = utilsObj.scanTable("alarm_history",pair.getKey(),pair.getValue(), connection);
                    utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                }catch (IOException e){
                    e.printStackTrace();
                }catch (ParseException e){
                    e.printStackTrace();
                }
            }
        }

        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>();
        allowUserIdsSet.addAll(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>();
        allowAlarmTypeSet.addAll(allowAlarmType);
        for(IAlarm element: queryResult){
            if(allowUserIdsSet.contains(element.getId())== false || allowAlarmTypeSet.contains(element.getType())==false){
                queryResult.remove(element);
            }

        }
        /**
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }
        return queryResult;
    }

    @Override
    public List<IAlarm> queryAlarmByImei(List<Long> imeis, int sortType, QueryFilter filter) {
        HashMap<Long, Pair<String, String>> rowkeyForQuery = new HashMap<Long, Pair<String, String>>();
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        rowkeyForQuery = utilsObj.createRowkeyForQueryByImei(imeis);

        // 警告表查询结果
        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));

        List<IAlarm> queryResult = new ArrayList<>();

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        try{
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor[] allTable = admin.listTables();

            for(HTableDescriptor oneTableDescriptor: allTable){
                String oneTableName = oneTableDescriptor.getNameAsString();
                for(Long imei: imeis){
                    try{
                        Pair<String, String> pair = rowkeyForQuery.get(imei);
                        Iterator<Result> results = utilsObj.scanTable(oneTableName,pair.getKey(),pair.getValue(), connection);
                        utilsObj.addToList(results, queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException e){
                        e.printStackTrace();
                    }catch (ParseException e){
                        e.printStackTrace();
                    }
                }

            }
        }catch (MasterNotRunningException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }


        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>();
        allowUserIdsSet.addAll(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>();
        allowAlarmTypeSet.addAll(allowAlarmType);
        for(IAlarm element: queryResult){
            if(allowUserIdsSet.contains(element.getId())== false || allowAlarmTypeSet.contains(element.getType())==false){
                queryResult.remove(element);
            }

        }
        /**
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }


        return queryResult;

    }

    @Override
    public void asyncQueryAlarmByUser(int qid, List<Integer> userBIds, boolean recursive, int sortType, QueryFilter filter) {
        IgniteSearch  igniteSearchObj = new IgniteSearch();

        // 用户id为user_id的所有子用户
        HashMap<Integer, ArrayList<Integer>> childrenOfUserB = new HashMap<Integer, ArrayList<Integer>>();
        HashMap<Integer, Long> directDevicesOfUserB = new HashMap<Integer, Long>();
        HashMap<Integer, ArrayList<Long>> imeiOfDevicesOfUserB = new HashMap<Integer, ArrayList<Long>>();

        // 创建使用AlarmSearchUtil的对象,方便操作
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        if(igniteSearchObj.connect()){
            for (Integer userBId : userBIds) {
                directDevicesOfUserB.putAll(utilsObj.getdirectDevicesOfUserB(userBId));
                imeiOfDevicesOfUserB.putAll(utilsObj.getImeiOfDevicesOfUserB(userBId));
            }
        }
        // 通过imeiOfDevicesOfUserB 查询用户的所有警告表

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        // 根据imei与创建时间与E创建行键rouKeys
        List<Pair<String, String>> rowKeys = new ArrayList<>();
        Date startTime = allowTimeRange.getKey();
        Date endTime = allowTimeRange.getValue();

        Random random = new Random();
        for(Long element: allowIMEIs) {
            String startRowKey = utilsObj.createRowKey(element, startTime, random.nextInt(10));
            String endRowKey = utilsObj.createRowKey(element, endTime, random.nextInt(10));
            Pair<String, String> pair = new Pair<>(startRowKey, endRowKey);
            rowKeys.add(pair);
        }

        // 警告表查询结果
        List<IAlarm> queryResult = new ArrayList<>();

        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
        Date date48before = new Date(Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4 - 48 * 1000L * 60 * 60 * 24);

        if(endTime.after(date48before)) {
            Date mmddstartTime = startTime.before(date48before) == true ? date48before : startTime;
            String endTableName = Utils.getTableName(endTime);

            while(!Utils.getTableName(mmddstartTime).equals(endTableName)) {
                for(Pair<String, String> pair: rowKeys){
                    try{
                        Iterator<Result> results = utilsObj.scanTable(Utils.getTableName(mmddstartTime),pair.getKey(),pair.getValue(), connection);
                        milliSecond = mmddstartTime.getTime() - Settings.BASETIME;
                        period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));
                        utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                        mmddstartTime = new Date(mmddstartTime.getTime()+ 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException e){
                        e.printStackTrace();
                    }catch (ParseException e){
                        e.printStackTrace();
                    }
                }
            }
        }
        if(startTime.before(date48before)){
            for(Pair<String, String> pair: rowKeys){
                try{
                    Iterator<Result> results = utilsObj.scanTable("alarm_history",pair.getKey(),pair.getValue(), connection);
                    utilsObj.addToList(results,startTime,endTime,queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                }catch (IOException e){
                    e.printStackTrace();
                }catch (ParseException e){
                    e.printStackTrace();
                }
            }
        }

        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>();
        allowUserIdsSet.addAll(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>();
        allowAlarmTypeSet.addAll(allowAlarmType);
        for(IAlarm element: queryResult){
            if(allowUserIdsSet.contains(element.getId())== false || allowAlarmTypeSet.contains(element.getType())==false){
                queryResult.remove(element);
            }

        }
        /**
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }
        // 将查询结果queryResult 地址可更改
        File f=new File(String.format("Query%d.txt", qid));
        try{
            BufferedWriter bw=new BufferedWriter(new FileWriter(f));
            for(int i=0;i<queryResult.size();i++){
                StringBuffer oneLine = new StringBuffer();
                oneLine.append(queryResult.get(i).getId()).append(" ");
                oneLine.append(queryResult.get(i).getImei()).append(" ");
                oneLine.append(queryResult.get(i).getStatus()).append(" ");
                oneLine.append(queryResult.get(i).getType()).append(" ");
                oneLine.append(queryResult.get(i).getLongitude()).append(" ");
                oneLine.append(queryResult.get(i).getLatitude()).append(" ");
                oneLine.append(queryResult.get(i).getVelocity()).append(" ");
                oneLine.append(queryResult.get(i).getAddress()).append(" ");
                oneLine.append(queryResult.get(i).getCreateTime()).append(" ");
                oneLine.append(queryResult.get(i).getPushTime()).append(" ");
                oneLine.append(queryResult.get(i).isViewed()).append(" ");
                oneLine.append(queryResult.get(i).getEncId());
                bw.write(oneLine.toString());
                bw.newLine();
            }
            bw.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public void asyncQueryAlarmByImei(int qid, List<Long> imeis, int sortType, QueryFilter filter) {
        HashMap<Long, Pair<String, String>> rowkeyForQuery = new HashMap<Long, Pair<String, String>>();
        AlarmSearchUtils utilsObj = new AlarmSearchUtils();
        rowkeyForQuery = utilsObj.createRowkeyForQueryByImei(imeis);

        // 警告表查询结果
        Calendar calendar = Calendar.getInstance();
        Date nowDate = calendar.getTime();

        long milliSecond = nowDate.getTime() - Settings.BASETIME;
        int period = (int)(milliSecond / (1000 * 60 * 60 * 24 * 4));

        List<IAlarm> queryResult = new ArrayList<>();

        // 四种过滤类型

        // 先使用allowIMEIs和allowTimeRange进行过滤
        List<Long> allowIMEIs = filter.getAllowIMEIs();
        Pair<Date, Date> allowTimeRange = filter.getAllowTimeRange();

        // 再使用allowUserIds和allowAlarmType进行过滤
        List<Integer> allowUserIds = filter.getAllowUserIds();
        List<String> allowAlarmType = filter.getAllowAlarmType();

        try{
            HBaseAdmin admin = new HBaseAdmin(config);
            HTableDescriptor[] allTable = admin.listTables();

            for(HTableDescriptor oneTableDescriptor: allTable){
                String oneTableName = oneTableDescriptor.getNameAsString();
                for(Long imei: imeis){
                    try{
                        Pair<String, String> pair = rowkeyForQuery.get(imei);
                        Iterator<Result> results = utilsObj.scanTable(oneTableName,pair.getKey(),pair.getValue(), connection);
                        utilsObj.addToList(results, queryResult,Settings.BASETIME + period * 1000L * 60 * 60 * 24 * 4);
                    }catch (IOException e){
                        e.printStackTrace();
                    }catch (ParseException e){
                        e.printStackTrace();
                    }
                }

            }
        }catch (MasterNotRunningException e){
            e.printStackTrace();
        }catch (IOException e){
            e.printStackTrace();
        }


        // 对查询结果queryResult,再使用allowUserIds和allowAlarmType进行过滤
        // List<IAlarm> queryResult = new ArrayList<>();
        Set<Integer> allowUserIdsSet = new HashSet<>();
        allowUserIdsSet.addAll(allowUserIds);
        Set<String> allowAlarmTypeSet = new HashSet<>();
        allowAlarmTypeSet.addAll(allowAlarmType);
        for(IAlarm element: queryResult){
            if(allowUserIdsSet.contains(element.getId())== false || allowAlarmTypeSet.contains(element.getType())==false){
                queryResult.remove(element);
            }

        }
        /**
         * sortType
         * ==1 IMEI
         * ==2 user_id
         * ==3 createTime
         * ==4 alarmType
         */
        if(sortType ==1 || sortType ==2 ||sortType ==3 ||sortType ==4){
            Collections.sort(queryResult, new Comparator<IAlarm>() {
                @Override
                public int compare(IAlarm A, IAlarm B) {
                    switch (sortType){
                        case 1:
                            if(A.getImei() < B.getImei())
                                return 1;
                            else
                                return 0;
                        case 2:
                            if(Long.valueOf(A.getId()) < Long.valueOf(B.getId()))
                                return 1;
                            else
                                return 0;
                        case 3:
                            if(A.getCreateTime().before(B.getCreateTime()))
                                return 1;
                            else
                                return 0;
                        case 4:
                            if(A.getType().compareTo(B.getType())<=0)
                                return 1;
                            else
                                return 0;
                        default:
                            return 0;
                    }
                }
            });
        }

        // 将查询结果queryResult 地址可更改
        File f=new File(String.format("Query%d.txt", qid));
        try{
            BufferedWriter bw=new BufferedWriter(new FileWriter(f));
            for(int i=0;i<queryResult.size();i++){
                StringBuffer oneLine = new StringBuffer();
                oneLine.append(queryResult.get(i).getId()).append(" ");
                oneLine.append(queryResult.get(i).getImei()).append(" ");
                oneLine.append(queryResult.get(i).getStatus()).append(" ");
                oneLine.append(queryResult.get(i).getType()).append(" ");
                oneLine.append(queryResult.get(i).getLongitude()).append(" ");
                oneLine.append(queryResult.get(i).getLatitude()).append(" ");
                oneLine.append(queryResult.get(i).getVelocity()).append(" ");
                oneLine.append(queryResult.get(i).getAddress()).append(" ");
                oneLine.append(queryResult.get(i).getCreateTime()).append(" ");
                oneLine.append(queryResult.get(i).getPushTime()).append(" ");
                oneLine.append(queryResult.get(i).isViewed()).append(" ");
                oneLine.append(queryResult.get(i).getEncId());
                bw.write(oneLine.toString());
                bw.newLine();
            }
            bw.close();
        }catch (IOException e){
            e.printStackTrace();
        }
    }

    @Override
    public List<IAlarm> queryAlarmByUserC(int userCId, int sortType) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByImeiStatus(int parentBId, boolean recursive) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserIdViewed(List<Integer> parentBIds, boolean recursive) {
        return null;
    }

    @Override
    public Map<String, Integer> groupCountByUserId(List<Integer> parentBIds, boolean recursive, int topK) {
        return null;
    }

    @Override
    public boolean close() {
        return false;
    }
}
