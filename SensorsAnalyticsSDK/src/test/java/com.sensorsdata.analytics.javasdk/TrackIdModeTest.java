package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * 迁移 idMapping 1.0 逻辑和支持自定义 _track_id 单元测试
 *
 * @author luowenjie
 * @version 3.4.3
 * @since 2022/04/20 15:50
 */
public class TrackIdModeTest extends SensorsBaseTest {
    private static final Map<String, Object> mp = new HashMap<>();
    private final String cookieId = "ABCDEF123456789";
    private final SensorsAnalyticsIdentity sensorsAnalyticsIdentity =
            SensorsAnalyticsIdentity.builder().addIdentityProperty("cookeId", cookieId).build();

    /** mp ：作为 properties 功能： 填充 map */
    @BeforeAll
    public static void buildMap() {
        mp.put("Channel", "baidu");
        mp.put("$time", new Date(1650785225));
        mp.put("$project", "default");
        mp.put("$token", "123");
        mp.put("$track_id", 123);
    }

    /** 测试功能：上报事件到 event 表 测试预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void track() throws InvalidArgumentException {
        EventRecord eventRecord =
                EventRecord.builder()
                        .setEventName("test1")
                        .isLoginId(false)
                        .addProperties(mp)
                        .setDistinctId(cookieId)
                        .build();
        sa.track(cookieId, false, "test1", mp);
        assertEventData(data);
        sa.track(eventRecord);
        assertEventData(data);
        sa.track(cookieId, false, "test");
        assertEventData(data);
    }

    /** 功能：检验 IDM 下上报事件属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void trackById() throws InvalidArgumentException {
        IDMEventRecord idmEventRecord =
                IDMEventRecord.starter()
                        .setEventName("trackById")
                        .addProperties(mp)
                        .addIdentityProperty("abc", cookieId)
                        .setDistinctId(cookieId)
                        .build();
        sa.trackById(idmEventRecord);
        assertEventData(data);
        sa.trackById(sensorsAnalyticsIdentity, "track", mp);
        assertEventData(data);
    }

    /** 功能：校验上报 $SignUp 事件 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void trackSignup() throws InvalidArgumentException {
        sa.trackSignUp("123", "dada");
        assertEventData(data);
        sa.trackSignUp("564", "daxfad", mp);
        assertEventData(data);
    }

    /** 功能：校验记录单个用户的属性信息 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileSet() throws InvalidArgumentException {
        UserRecord userRecord =
                UserRecord.builder()
                        .setDistinctId(cookieId)
                        .addProperties(mp)
                        .isLoginId(Boolean.FALSE)
                        .build();
        sa.profileSet(userRecord);
        assertUserData(data);
        sa.profileSet(cookieId, true, mp);
        assertUserData(data);
        sa.profileSet(cookieId, true, "$track_id", 3131);
        assertProperties(data);
    }

    /** 功能：IDM 下校验记录单个用户的属性信息 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileSetById() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter().setDistinctId(cookieId).addProperties(mp).build();
        sa.profileSetById(userRecord);
        assertUserData(data);
        sa.profileSetById(sensorsAnalyticsIdentity, mp);
        assertUserData(data);
        sa.profileSetById(sensorsAnalyticsIdentity, "$track_id", 123);
        assertUserData(data);
    }

    /** 功能：校验首次设置用户属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileSetOnce() throws InvalidArgumentException {
        UserRecord userRecord =
                UserRecord.builder()
                        .setDistinctId(cookieId)
                        .addProperties(mp)
                        .isLoginId(Boolean.FALSE)
                        .build();
        sa.profileSetOnce(userRecord);
        assertUserData(data);
        sa.profileSetOnce("ABCDEF123456789", false, mp);
        assertUserData(data);
        sa.profileSetOnce(cookieId, false, "$track_id", 123);
        assertUserData(data);
    }

    /** 功能：IDM 下校验首次设置用户属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileSetOnceById() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter().setDistinctId(cookieId).addProperties(mp).build();
        sa.profileSetOnceById(userRecord);
        assertUserData(data);
        sa.profileSetOnceById(sensorsAnalyticsIdentity, mp);
        assertUserData(data);
        sa.profileSetOnceById(sensorsAnalyticsIdentity, "$track_id", 123);
        assertUserData(data);
    }

    /** 功能：校验删除指定用户已存在的一条或者多条属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileUnset() throws InvalidArgumentException {
        Map<String, Object> mp = new HashMap<>();
        mp.put("Channel", true);
        UserRecord userRecord =
                UserRecord.builder()
                        .setDistinctId(cookieId)
                        .addProperties(mp)
                        .isLoginId(Boolean.FALSE)
                        .build();
        sa.profileUnset(userRecord);
        assertUserData(data);
        sa.profileUnset(cookieId, true, mp);
        assertUserData(data);
        sa.profileUnset(cookieId, true, "Channel");
        assertUserData(data);
    }

    /**
     * 功能：IDM 下校验删除指定用户已存在的一条或者多条属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id
     */
    @Test
    public void profileUnsetById() throws InvalidArgumentException {
        Map<String, Object> ma = new HashMap<>();
        ma.put("Channel", true);
        IDMUserRecord idmUserRecord =
                IDMUserRecord.starter().setDistinctId(cookieId).addProperties(ma).build();
        sa.profileUnsetById(idmUserRecord);
        assertUserData(data);
        sa.profileUnsetById(sensorsAnalyticsIdentity, ma);
        assertUserData(data);
        sa.profileUnsetById(sensorsAnalyticsIdentity, "Channel");
        assertUserData(data);
    }

    /** 功能：校验删除指定用户所有属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileDelete() throws InvalidArgumentException {
        UserRecord userRecord =
                UserRecord.builder().setDistinctId(cookieId).isLoginId(true).build();
        sa.profileDelete(cookieId, true);
        assertUserData(data);
        sa.profileDelete(userRecord);
        assertUserData(data);
    }

    /** 功能：IDM 下校验删除指定用户所有属性 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void profileDeleteById() throws InvalidArgumentException {
        sa.profileDeleteById(sensorsAnalyticsIdentity);
        assertUserData(data);
    }

    /**
     * 功能：校验为指定用户的一个或多个数组类型的属性追加字符串 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id
     */
    @Test
    public void profileAppend() throws InvalidArgumentException {
        List<String> newInterest = new ArrayList<>();
        newInterest.add("ball");
        Map<String, Object> mp = new HashMap<>();
        mp.put("interest", newInterest);
        UserRecord appendRecord =
                UserRecord.builder()
                        .setDistinctId(cookieId)
                        .isLoginId(Boolean.TRUE)
                        .addProperty("interest", newInterest)
                        .build();
        sa.profileAppend(appendRecord);
        assertUserData(data);
        sa.profileAppend(cookieId, false, mp);
        assertUserData(data);
        sa.profileAppend(cookieId, false, "interest", "ball");
        assertUserData(data);
    }

    /**
     * 功能：IDM 下校验为指定用户的一个或多个数组类型的属性追加字符串 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和
     * $track_id
     */
    @Test
    public void profileAppendById() throws InvalidArgumentException {
        List<String> newInterest = new ArrayList<>();
        newInterest.add("ball");
        Map<String, Object> mp = new HashMap<>();
        mp.put("interest", newInterest);
        IDMUserRecord idmUserRecord =
                IDMUserRecord.starter()
                        .setDistinctId(cookieId)
                        .addProperty("interest", newInterest)
                        .build();
        sa.profileAppendById(idmUserRecord);
        assertUserData(data);
        sa.profileAppendById(sensorsAnalyticsIdentity, mp);
        assertUserData(data);
        sa.profileAppendById(sensorsAnalyticsIdentity, "interest", "ball");
        assertUserData(data);
    }

    /**
     * 功能：校验为指定用户的一个或多个数值类型的属性累加一个数值 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和 $track_id
     */
    @Test
    public void profileIncrement() throws InvalidArgumentException {
        Map<String, Object> mp = new HashMap<>();
        mp.put("age", 2);
        UserRecord incrementRecord =
                UserRecord.builder()
                        .setDistinctId(cookieId)
                        .isLoginId(Boolean.TRUE)
                        .addProperty("age", 2)
                        .build();
        sa.profileIncrement(incrementRecord);
        assertUserData(data);
        sa.profileIncrement(cookieId, false, mp);
        assertUserData(data);
        sa.profileIncrement(cookieId, false, "age", 2);
        assertUserData(data);
    }

    /**
     * 功能：IDM 下校验为指定用户的一个或多个数值类型的属性累加一个数值 预期：生成的数据符合神策格式,且 _track_id 存在，properties 中不包含 $time 和
     * $track_id
     */
    @Test
    public void profileIncrementById() throws InvalidArgumentException {
        IDMUserRecord idmUserRecord =
                IDMUserRecord.starter().setDistinctId(cookieId).addProperty("age", 2).build();
        Map<String, Object> mp = new HashMap<>();
        mp.put("age", 2);
        sa.profileIncrementById(idmUserRecord);
        assertUserData(data);
        sa.profileIncrementById(sensorsAnalyticsIdentity, mp);
        assertUserData(data);
        sa.profileIncrementById(sensorsAnalyticsIdentity, "age", 2);
        assertUserData(data);
    }

    /** 功能：校验增加 item 记录 预期：生成的数据符合神策格式,且 _track_id 不存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void itemSet() throws InvalidArgumentException {
        Map<String, Object> mp = new HashMap<>();
        mp.put("$track_id", 123);
        mp.put("$time", new Date(1650785226));
        ItemRecord itemRecord =
                ItemRecord.builder()
                        .setItemType("test")
                        .setItemId(cookieId)
                        .addProperties(mp)
                        .build();
        sa.itemSet(itemRecord);
        assertItemData(data);
        sa.itemSet("test", cookieId, mp);
        assertItemData(data);
    }

    /** 功能：校验删除 item 记录 预期：生成的数据符合神策格式,且 _track_id 不存在，properties 中不包含 $time 和 $track_id */
    @Test
    public void itemDelete() throws InvalidArgumentException {
        Map<String, Object> mp = new HashMap<>();
        mp.put("$track_id", 123);
        mp.put("$time", new Date(1650785226));
        ItemRecord itemRecord =
                ItemRecord.builder()
                        .setItemType("test")
                        .setItemId(cookieId)
                        .addProperties(mp)
                        .build();
        sa.itemDelete(itemRecord);
        assertItemData(data);
        sa.itemDelete("test", cookieId, mp);
        assertItemData(data);
    }
}
