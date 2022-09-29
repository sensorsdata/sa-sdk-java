package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  适用于 v3.4.4+ 版本
 *  测试点：验证事件的 _track_id 正常由 $track_id 生成，time 由 $time 生成。
 *  无特殊情况，不在下面的 testcase 上一一说明
 */
public class IDMappingModel3TestTrackIdAndTime extends SensorsBaseTest {
    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    SensorsAnalytics saTmp;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        String url = "http://10.120.111.143:8106/sa?project=default";
        // 注意要设置 bulkSize 稍微大一点，这里设置为 100，否则超过 1 条就上报，messageList 里面拿不到事件数据
        batchConsumer = new BatchConsumer(url, 100, true, 3);
        // 通过反射机制获取 BatchConsumer 的 messageList
        Field field = batchConsumer.getClass().getDeclaredField("messageList"); // messageList 是 BatchConsumer 用来暂存事件数据的成员变量
        field.setAccessible(true);
        messageList = (List<Map<String, Object>>) field.get(batchConsumer);
        saTmp = new SensorsAnalytics(batchConsumer);
    }

    private void assertNotNullProp(){
        assertNotNull(messageList.get(0).get("identities"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertNotNull(messageList.get(0).get("properties"));
    }

    @Test
    public void testSingleIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));
        assertEquals(date.getTime(), messageList.get(0).get("time"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    }


    /**
     * 校验 ID-Mapping bind 接口
     */
    @Test
    public void testIdMappingBind() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("$identity_mobile", "123")
                .addIdentityProperty("$identity_email", "fz@163.com")
                .build();
        saTmp.bind(identity);
    }


    /**
     * 校验 ID-Mapping unbind 接口用户格式
     */
    @Test
    public void checkUnbindUserId() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.unbind(identity);

    }

    /**
     * 【已知问题】https://jira.sensorsdata.cn/browse/SDK-4863
     *  【Java SDK】【_track_id】公共属性设置 $time, 属性中也带 $time，同时这条事件的 time 也会由 $time 生成
     */
    @Test
    @Ignore
    public void checkTrackByIdSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);

        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的
        assertNotEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $time 不生效的


        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性
    }

    /**
     * 【已知问题】https://jira.sensorsdata.cn/browse/SDK-4863
     *  【Java SDK】【_track_id】公共属性设置 $time, 属性中也带 $time，同时这条事件的 time 也会由 $time 生成
     */
    @Test
    @Ignore
    public void checkTrackByIdSuperProperties01() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", "aaa");
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);

        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals("aaa", messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的
        assertNotEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $time 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性
    }


    /**
     * 【已知问题】https://jira.sensorsdata.cn/browse/SDK-4863
     *  【Java SDK】【_track_id】公共属性设置 $time, 属性中也带 $time，同时这条事件的 time 也会由 $time 生成
     */
    @Test
    public void checkBindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .addIdentityProperty("eee", "123")
                .build();
        saTmp.bind(identity);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertNotEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性

    }

    /**
     * 【已知问题】https://jira.sensorsdata.cn/browse/SDK-4863
     *  【Java SDK】【_track_id】公共属性设置 $time, 属性中也带 $time，同时这条事件的 time 也会由 $time 生成
     */
    @Test
    public void checkUnbindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.unbind(identity);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertNotEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     */
    @Test
    public void testTrackByIdIDMEventRecord() throws InvalidArgumentException  {
        Date date = new Date();
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .setDistinctId("disId123")
                .setEventName("test")
                .addProperty("$track_id", 111)
                .addProperty("$time", date)
                .addProperty("abc", 111)
                .build();

        saTmp.trackById(record);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));
        assertEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性
    }


    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetById() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.profileSetById(identity, properties);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));
        assertEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnesById00() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        saTmp.profileSetOnceById(identity, properties);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));
        assertEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementById00(){
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("age", 1);
            properties.put("$track_id", 111);
            Date date = new Date();
            properties.put("$time", date);

            saTmp.profileIncrementById(identity, properties);
            fail("profileIncrementById should throw InvalidArgumentException.");
        }catch (InvalidArgumentException e){
            assertEquals("The property value of PROFILE_INCREMENT should be a Number.The current type is class java.util.Date.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void testProfileAppendById() {
        try{
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            List<String> list = new ArrayList<>();
            list.add("apple");
            list.add("orange");
            Map<String, Object> properties = new HashMap<>();
            properties.put("favorite", list);
            properties.put("$track_id", 111);
            Date date = new Date();
            properties.put("$time", date);
            saTmp.profileAppendById(identity, properties);

            fail("profileAppendById should throw InvalidArgumentException.");
        }catch(InvalidArgumentException e){
            String msg = "The property value of PROFILE_APPEND should be a List<String>.";
            assertEquals(msg, e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void testProfileUnsetById() {
        try{
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            List<String> list = new ArrayList<>();
            list.add("apple");
            list.add("orange");
            Map<String, Object> properties = new HashMap<>();
            properties.put("favorite", true);
            properties.put("$track_id", 111);
            Date date = new Date();
            properties.put("$time", date);

            saTmp.profileUnsetById(identity, properties);

            fail("profileUnsetById should throw InvalidArgumentException.");
        }catch(InvalidArgumentException e){
            String msg = "The property value of [$time] should be true.";
            assertEquals(msg, e.getMessage());
            e.printStackTrace();
        }
    }

    // profileDeleteById
    // TODO
    @Test
    public void testProfileDeleteById() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();

        saTmp.profileDeleteById(identity);
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNew() throws InvalidArgumentException {
        // 新版本接口
        Date date = new Date();
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111)
                .addProperty("$time", date) // 设置 $track_id
                .build();
        saTmp.profileSetById(userRecord);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));
        assertEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNew() throws InvalidArgumentException {
        // 新版本接口
        Date date = new Date();
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111)
                .addProperty("$time", date) // 设置 $track_id
                .build();
        saTmp.profileSetOnceById(userRecord);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));
        assertEquals(date.getTime(), messageList.get(0).get("time")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew() {
        // 新版本接口
        Date date = new Date();
        IDMUserRecord userRecord = null;
        try {
            userRecord = IDMUserRecord.starter()
                    .setDistinctId("xc001") //手动指定外层 distinct_id
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                    .addProperty("age", 1) // 设置埋点事件属性
                    .addProperty("$track_id", 111)
                    .addProperty("$time", date) // 设置 $track_id
                    .build();
            saTmp.profileIncrementById(userRecord);
            fail("profileIncrementById should throw InvalidArgumentException.");
        } catch (InvalidArgumentException e) {
            assertEquals("The property value of PROFILE_INCREMENT should be a Number.The current type is class java.util.Date.", e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendByIdNew() {
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        Date date = new Date();
        IDMUserRecord userRecord = null;
        try {
            userRecord = IDMUserRecord.starter()
                    .setDistinctId("xc001") //手动指定外层 distinct_id
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                    .addProperty("favorite", list) // 设置埋点事件属性
                    .addProperty("$track_id", 111)
                    .addProperty("$time", date) // 设置 $track_id
                    .build();
            saTmp.profileAppendById(userRecord);
            fail("profileAppendById should throw InvalidArgumentException.");
        }catch(InvalidArgumentException e){
            String msg = "The property value of PROFILE_APPEND should be a List<String>.";
            assertEquals(msg, e.getMessage());
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNew() {
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        Date date = new Date();
        IDMUserRecord userRecord = null;
        try {
            userRecord = IDMUserRecord.starter()
                    .setDistinctId("xc001") //手动指定外层 distinct_id
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                    .addProperty("favorite", true) // 设置埋点事件属性
                    .addProperty("$track_id", 111)
                    .addProperty("$time", date) // 设置 $track_id
                    .build();
            saTmp.profileUnsetById(userRecord);
            fail("profileUnsetById should throw InvalidArgumentException.");
        }catch(InvalidArgumentException e){
            String msg = "The property value of $time should be true.";
            assertEquals(msg, e.getMessage());
            e.printStackTrace();
        }
    }

}
