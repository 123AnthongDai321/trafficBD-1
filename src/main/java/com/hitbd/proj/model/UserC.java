package com.hitbd.proj.model;

import java.util.ArrayList;
import java.util.List;

import com.hitbd.proj.model.IUserC;

public class UserC<E> implements IUserC {
    private int user_id;
    private StringBuffer devices;
    private StringBuffer authed_device;
    private StringBuffer auth_user_ids;

    /**
     * @param user_id
     */
    public UserC(int user_id, String devices, String authed_device, String auth_user_ids) {
        super();
        this.user_id = user_id;
        this.devices = new StringBuffer(devices);
        this.authed_device = new StringBuffer(authed_device);
        this.auth_user_ids = new StringBuffer(auth_user_ids);
    }


    /*
     * @param the user_id
     * */
    public void setUserCid(int user_id) {
        this.user_id = user_id;
    }

    /*
     *  @param the user_id
     */
    public void setDevices(String devices) {
        this.devices = new StringBuffer(devices);
    }

    /*
     *  @param the user_id
     */
    public void setAuthed_device(String authed_device) {
        this.authed_device = new StringBuffer(authed_device);
    }

    /*
     *  @param the user_id
     */
    public void setAuth_user_ids(String auth_user_ids) {
        this.auth_user_ids = new StringBuffer(auth_user_ids);
    }

    public int getUserCId() {
        return this.user_id;
    }

    public String getAuthed_deviceString() {
        return new String(authed_device);
    }

    public String getAuth_user_idsString() {
        return new String(auth_user_ids);
    }

    public String getDevicesString() {
        return new String(auth_user_ids);
    }

    @Override
    public List<Long> getDevices() {
        String[] temp = new String(this.devices).split(",");
        ArrayList<Long> result = new ArrayList<Long>();
        for (int i = 0; i < temp.length; i++) {
            result.add(Long.valueOf(temp[i]));
        }
        return result;
    }

    @Override
    public void setDevices(List<Long> devices) {
        String result = new String();
        for (Long dev : devices) {
            result += ("dev" + ",");
        }
        this.devices = new StringBuffer(result);
    }

    @Override
    public List<Long> getAuthedDevices() {
        String[] temp = new String(authed_device).split(",");
        ArrayList<Long> result = new ArrayList<Long>();
        for (int i = 0; i < temp.length; i++) {
            result.add(Long.valueOf(temp[i]));
        }
        return result;
    }

    @Override
    public void setAuthedDevices(List<Long> authedDevices) {
        String result = new String();
        for (Long dev : authedDevices) {
            result += ("dev" + ",");
        }
        this.authed_device = new StringBuffer(result);
    }

    @Override
    public List<Integer> getAuthUserIds() {
        String[] temp = new String(auth_user_ids).split(",");
        ArrayList<Integer> result = new ArrayList<Integer>();
        for (int i = 0; i < temp.length; i++) {
            result.add(Integer.valueOf(temp[i]));
        }
        return result;
    }

    @Override
    public void setAuthUserIds(List<Integer> authUserIds) {
        String result = new String();
        for (Integer dev : authUserIds) {
            result += ("dev" + ",");
        }
        this.auth_user_ids = new StringBuffer(result);
    }
}
