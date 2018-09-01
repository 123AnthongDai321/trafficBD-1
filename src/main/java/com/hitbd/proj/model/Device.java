package com.hitbd.proj.model;

import java.util.Date;
import java.util.List;

public class Device implements IDevice {
    private int userBId;
    private long imei;
    private StringBuffer deviceType;
    private StringBuffer undifined;
    private StringBuffer projectId;
    private boolean enabled;
    private boolean repayment;
    private StringBuffer expire_list;
    List<Pair<Integer, Date>> expireList;
    private int user_c_id;

    public Device(int userBId, long imei, String deviceType, String undifined) {
        super();
        this.userBId = userBId;
        this.imei = imei;
        this.deviceType = new StringBuffer(deviceType);
        this.setUndifined(undifined);
    }

    public Device(int userBId, long imei) {
        super();
        this.userBId = userBId;
        this.imei = imei;
    }

    /**
     * @return imei
     */
    public long getImei() {
        return imei;
    }

    /**
     * @param imei 要设置的 imei
     */
    public void setImei(long imei) {
        this.imei = imei;
    }

    /**
     * @param enabled 要设置的 enabled
     */
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    /**
     * @param repayment 要设置的 repayment
     */
    public void setRepayment(boolean repayment) {
        this.repayment = repayment;
    }

    /**
     * @return expire_list
     */
    public String getExpire_list() {
        return new String(expire_list);
    }

    /**
     * @param expire_list 要设置的 expire_list
     */
    public void setExpire_list(String expire_list) {
        this.expire_list = new StringBuffer(expire_list);
    }

    /**
     * @return undifined
     */
    public String getUndifined() {
        return new String(undifined);
    }

    /**
     * @param undifined 要设置的 undifined
     */
    public void setUndifined(String undifined) {
        this.undifined = new StringBuffer(undifined);
    }

    @Override
    public int getUserBId() {
        return this.userBId;
    }

    @Override
    public void setUserBId(int bId) {
        this.userBId = bId;
    }

    @Override
    public int getUserCId() {
        return this.user_c_id;
    }

    @Override
    public void setUserCId(int Cid) {
        this.user_c_id = Cid;
    }

    @Override
    public String getDeviceType() {
        return new String(this.deviceType);
    }

    @Override
    public void setDeviceType(String deviceType) {
        this.deviceType = new StringBuffer(deviceType);
    }

    @Override
    public String getDeviceName() {
        return new String(this.undifined);
    }

    @Override
    public void setDeviceName(String deviceName) {
        this.undifined = new StringBuffer(deviceName);
    }

    @Override
    public String getProjectId() {
        return new String(this.projectId);
    }

    @Override
    public void setProjectId(String projectId) {
        this.projectId = new StringBuffer(projectId);
    }

    @Override
    public boolean getEnabled() {
        return this.enabled;
    }

    @Override
    public boolean getRepayment() {
        return this.repayment;
    }

    @Override
    public List<Pair<Integer, Date>> getExpireList() {
        return this.expireList;
    }

    @Override
    public void setExpireList(List<Pair<Integer, Date>> expireList) {
        this.expireList = expireList;
    }

}
