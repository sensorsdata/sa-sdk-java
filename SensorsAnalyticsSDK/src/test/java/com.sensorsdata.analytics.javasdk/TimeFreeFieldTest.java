package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 检查 time_free 生成逻辑 1、公共属性中设置 2、自定义属性中设置 3、同时设置，以公共属性中设置为准
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/12/19 17:20
 */
public class TimeFreeFieldTest extends SensorsBaseTest {

    @Test
    public void trackTest() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        sa.track("test1", true, "test", pro);
        assertEventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertTrue(properties.containsKey("key1"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void trackTest1() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        EventRecord eventRecord =
                EventRecord.builder()
                        .setDistinctId("test1")
                        .isLoginId(false)
                        .setEventName("test")
                        .addProperties(pro)
                        .build();
        sa.track(eventRecord);
        assertEventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertTrue(properties.containsKey("key1"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void trackTest2() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", false);
        sa.track("test1", true, "test", pro);
        assertEventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(data.containsKey("time_free"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void profileTest() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        sa.profileSet("test1", true, pro);
        assertUserData(data);
        Assertions.assertFalse(data.containsKey("time_free"));
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void profileTest1() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        UserRecord userRecord =
                UserRecord.builder()
                        .setDistinctId("test1")
                        .isLoginId(true)
                        .addProperties(pro)
                        .build();
        sa.profileSet(userRecord);
        assertUserData(data);
        Assertions.assertFalse(data.containsKey("time_free"));
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void itemTest() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        sa.itemSet("test1", "aa1", pro);
        assertItemData(data);
        Assertions.assertFalse(data.containsKey("time_free"));
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void itemTest1() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        ItemRecord itemRecord =
                ItemRecord.builder()
                        .setItemId("test1")
                        .setItemType("aa1")
                        .addProperties(pro)
                        .build();
        sa.itemSet(itemRecord);
        assertItemData(data);
        Assertions.assertFalse(data.containsKey("time_free"));
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void IDMTrackTest() throws InvalidArgumentException {
        IDMEventRecord eventRecord =
                IDMEventRecord.starter()
                        .setEventName("test1")
                        .setDistinctId("ee1")
                        .addIdentityProperty("test", "e22ee")
                        .addProperty("$time_free", true)
                        .build();
        sa.trackById(eventRecord);
        assertIDM3EventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void IDMTrackTest1() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder().addIdentityProperty("test1", "ee").build();
        sa.trackById(identity, "test1", pro);
        assertIDM3EventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void IDMTrackTest2() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", false);
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder().addIdentityProperty("test1", "ee").build();
        sa.trackById(identity, "test1", pro);
        assertIDM3EventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(data.containsKey("time_free"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void IDMProfileTest() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty("test1", "ee")
                        .addProperties(pro)
                        .build();
        sa.profileSetById(userRecord);
        assertIDM3UserData(data);
        Assertions.assertFalse(data.containsKey("time_free"));
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void NGUserEventTrackTest() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        UserEventSchema userEventSchema =
                UserEventSchema.init()
                        .addIdentityProperty("key1", "ee1")
                        .setEventName("test1")
                        .addProperties(pro)
                        .start();
        sa.track(userEventSchema);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void NGItemEventTrackTest() throws InvalidArgumentException {
        ItemEventSchema itemEventSchema =
                ItemEventSchema.init()
                        .setSchema("test1")
                        .setItemPair("id_1", "ree")
                        .setEventName("test3")
                        .addProperty("$time_free", true)
                        .start();
        sa.track(itemEventSchema);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void NGUserProfileTest() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init().setUserId(22L).addProperty("$time_free", true).start();
        sa.profileSet(userSchema);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(data.containsKey("time_free"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void NGItemTest() throws InvalidArgumentException {
        ItemSchema itemSchema =
                ItemSchema.init()
                        .setSchema("test33")
                        .setItemId("ee2")
                        .addProperty("$time_free", true)
                        .start();
        sa.itemSet(itemSchema);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertFalse(data.containsKey("time_free"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }

    @Test
    public void checkCommonProtest() throws InvalidArgumentException {
        Map<String, Object> pro = new HashMap<>();
        pro.put("key1", "aaa");
        pro.put("$time_free", true);
        sa.registerSuperProperties(pro);
        sa.track("test1", true, "test", null);
        assertEventData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(Boolean.parseBoolean(data.get("time_free").toString()));
        Assertions.assertTrue(properties.containsKey("key1"));
        Assertions.assertFalse(properties.containsKey("$time_free"));
    }
}
