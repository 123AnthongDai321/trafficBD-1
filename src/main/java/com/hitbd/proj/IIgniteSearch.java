package com.hitbd.proj;

import com.hitbd.proj.Exception.DuplicatedPKException;
import com.hitbd.proj.Exception.ForeignKeyException;
import com.hitbd.proj.Exception.NotExistException;
import com.hitbd.proj.Exception.TimeException;
import com.hitbd.proj.model.IDevice;
import com.hitbd.proj.model.IUserB;
import com.hitbd.proj.model.IUserC;

import java.sql.SQLException;
import java.util.Date;

/*
 * 本文件不允许擅自修改，有修改需求请联系负责人
 */
public interface IIgniteSearch {
    /**
     * C5.1
     * 连接到Ignite
     * @return
     */
    boolean connect();

    /**
     * C5.1
     * 使用特定的IP和端口号连接到Ignite
     * @param hostname Ignite节点地址
     * @param port 端口
     * @return
     */
    boolean connect(String hostname, int port);

    /**
     * C5.2
     * 获取设备imei的累计告警计数值
     * @param imei
     * @return
     */
    int getAlarmCount(long imei);

    /**
     * C5.2
     * 修改设备imei的累计告警计数值
     * @param imei
     * @param count
     */
    void setAlarmCount(long imei, int count);

    /**
     * C5.2
     * 获取设备imei的累计已读告警计数值
     * @param imei
     * @return
     */
    int getViewedCount(long imei);

    /**
     * C5.2
     * 获取设备imei的累计已读告警计数值
     * @param imei
     * @param count
     */
    void setViewedCount(long imei, int count);

    /**
     * C5.3
     * 获取一个B端用户
     * @param userBId
     * @return
     * @throws NotExistException 该b端用户不存在时抛出此异常
     */
    IUserB getUserB(int userBId) throws NotExistException;

    /**
     * 根据用户id查找直接设备
     * @param user_b_id
     * @return IDevice
     * @throws NotExistException
     * 备注：代洋洋增加
     */
    IDevice getDirectDevice(int user_b_id) throws NotExistException;

    /**
     * C5.3
     * 获取一个设备
     * @param imei
     * @return
     * @throws NotExistException 该设备不存在时抛出异常
     */
    IDevice getDevice(int imei) throws NotExistException;

    /**
     * C5.3
     * 获取一个C端用户
     * @param userCId
     * @return
     * @throws NotExistException 该C端用户不存在时抛出异常
     */
    IUserC getUserC(int userCId) throws NotExistException;

    /**
     * No3.1
     * 新建一个b端用户
     * @param parentId 该b端用户的父用户id，如果没有父用户，设置为-1
     * @return 新增用户的id
     */
    int createUserB(int parentId);

    /**
     * No3.2
     * 新建一个c端用户
     * @return 新增用户的id
     */
    int createUserC();

    /**
     * No3.3
     * 新增一个设备
     * @param imei 该设备的imei号
     * @param userBId 该设备直属的b端用户
     * @throws DuplicatedPKException 该imei号与其他设备的imei号重合时抛出异常
     * @throws ForeignKeyException 设置的直属b端用户不存在或数值非法(小于0)时抛出异常
     */
    void createDevice(long imei, int userBId) throws DuplicatedPKException, ForeignKeyException;

    /**
     * No3.5
     * 新增一个父子关系
     * @param childBId 子b端用户id
     * @param parentBId 父b端用户id
     * @throws ForeignKeyException 子用户不得已经存在父用户，存在时抛出异常
     * @throws NotExistException 输入id必须都是b端用户表中已经存在的用户，否则抛出异常
     */
    void setNewParent(int childBId, int parentBId) throws ForeignKeyException, NotExistException;

    /**
     * No3.6
     * 为C端用户增加拥有的设备
     * @param userCId C端用户id
     * @param imei 拥有的设备
     * @throws ForeignKeyException 该设备已被另一个C端用户拥有时抛出异常
     * @throws NotExistException 该设备或该C端用户不存在时抛出异常
     */
    void addUserCDevice(int userCId, long imei) throws ForeignKeyException, NotExistException;

    /**
     * No3.8
     * 将imei授权给用户c
     * @param imei 待授权的设备
     * @param toCId 得到授权的C端用户
     * @throws ForeignKeyException 得到授权的C端用户不得是该设备的拥有者，否则抛出此异常
     * @throws NotExistException 设备号或C端用户id不存在时抛出此异常
     */
    void authorizeCDevice(long imei, int toCId) throws ForeignKeyException, NotExistException;

    /**
     * No4.1
     * 修改设备属性
     * @param imei
     * @param deviceType
     * @param deviceName
     * @param projectId
     * @param enabled
     * @param repayment
     * @throws SQLException 该设备不存在时抛出此异常
     */
    public void updateDevice(long imei, String deviceType, String deviceName, String projectId, boolean enabled,
                             boolean repayment, String isupdate) throws SQLException;

    /**
     * No4.3
     * 删除父子关系
     * @param childBId 被删除的父子关系中的儿子
     * @throws SQLException 该id不存在或该id没有父用户时抛出异常
     * @throws NotExistException 
     */
    void deleteParentLink(int childBId) throws SQLException, NotExistException;

    /**
     * No4.4
     * 将一个用户的设备转移给另一个用户
     * @param imei 被转移的设备
     * @param toBid 被转移到的用户
     * @param parentIds 新父用户id
     * @param expireDates 新父用户对应的过期时间
     * @throws ForeignKeyException 设备或用户不存在时抛出异常
     * @throws TimeException 
     */
    void relocateDevice(long imei, int toBid, String[] parentIds, Date[] expireDates) throws SQLException, ForeignKeyException, TimeException;

    /**
     * No4.6
     * 删除一个设备
     * @param imei
     * @throws SQLException 设备不存在时抛出异常
     */
    void deleteDevice(long imei) throws SQLException;

    /**
     * No4.7
     * 删除一个用户的设备授权，但不会删除这个设备
     * @param imei 被移除授权的设备
     * @param userCId 被移除授权的用户
     * @throws SQLException 用户或设备不存在时抛出异常
     */
    void deleteAuthorization(long imei, int userCId) throws SQLException;

    /**
     * No4.8
     * 修改一个设备关于它某个父用户的过期时间
     * @param imei 设备号
     * @param userBId 这个设备的一个父用户
     * @param expireDate 新过期时间
     * @throws NotExistException 用户或设备不存在，或该设备不是该用户的父用户时抛出异常
     * @throws TimeException 时间不合法（早于2010年1月1日时抛出异常）
     */
    void updateExpireDate(long imei, int userBId, Date expireDate) throws NotExistException, TimeException;

    /**
     * 4.10
     * 移除一个设备与其C端用户的拥有关系，移除时不会删除该C端用户或设备
     * @param imei
     * @throws SQLException 用户或设备不存在时抛出异常
     */
    void removeCDevice(long imei) throws SQLException;

    /**
     * C5.1
     * 断开与Ignite的连接
     * @return
     */
    boolean close();
}
