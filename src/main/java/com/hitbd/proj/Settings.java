package com.hitbd.proj;

import java.util.Date;

public class Settings {
    public static long BASETIME = 1262275200000L; //2010年1月1日 00:00:00
    public static String QUERY_LOG_FILENAME = "Query%d.txt"; //查询结果文件名
    public static String igniteHostAddress = "127.0.0.1";
    public static String logDir = "./";
    public static Date MAXTIME = new Date(199,1,1);
}
