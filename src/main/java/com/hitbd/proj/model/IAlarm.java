package com.hitbd.proj.model;

import java.util.Date;

public interface IAlarm {

  String getId();

  void setId(String id);

  long getImei();

  void setImei(long imei);

  String getStatus();

  void setStatus(String status);

  String getType();

  void setType(String type);

  float getLongitude();

  void setLongitude(float longitude);

  float getLatitude();

  void setLatitude(float latitude);

  float getVelocity();

  void setVelocity(float velocity);

  String getAddress();

  void setAddress(String address);

  Date getCreateTime();

  void setCreateTime(Date date);

  Date getPushTime();

  void setPushTime(Date date);

  boolean isViewed();

  void setViewed(boolean viewed);

  String getEncId();

  void setEncId(String encId);

  /**
   * A 获取该记录的行键
   * 
   * @return
   */
  String getRowKey();

  /**
   * A 获取该记录所存在的表名
   * 
   * @return
   */
  String getTableName();
}
