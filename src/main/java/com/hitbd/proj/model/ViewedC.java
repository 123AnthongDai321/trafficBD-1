package com.hitbd.proj.model;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;

public class ViewedC {
    static private Ignite ignite = Ignition.start();
    static private IgniteCache<Long, Integer> cache;

    public void createCache() {
        CacheConfiguration<Long, Integer> cfg = new CacheConfiguration<Long, Integer>();
        cfg.setName("Test");
        cfg.setCacheMode(CacheMode.PARTITIONED);//存储方式 PARTITIONED适合分布式存储
        cfg.setIndexedTypes(Long.class, Integer.class); //必须设置索引类否则只能以key-value方式查询
        cache = ignite.getOrCreateCache(cfg);//根据配置创建缓存
    }

    public int getViewedCount(long imei) {
        return cache.get(imei);
    }

    public void setViewedCount(long imei, int count) {
        if (!cache.containsKey(imei)) {
            cache.put(imei, count);
        } else {
            cache.replace(imei, count);
        }
    }

    public static void main(String[] args) {
        ViewedC almc = new ViewedC();
        almc.createCache();
        for (long i = 0; i < 8; i++) {
            almc.setViewedCount(i, (int) (i + 88));
        }
        for (long i = 0; i < 8; i++) {
            System.out.println(almc.getViewedCount(i));
        }
    }

}
