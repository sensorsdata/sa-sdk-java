package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.DebugConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;

/**
 *  适用于 v3.4.2+ 版本
 *  测试数据是否能正常上传
 */
public class IDMappingModel3TestTrackID extends SensorsBaseTest {
    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    SensorsAnalytics saTmp;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
//        String url = "http://10.120.73.51:8106/sa?project=default&token=";
//        String url = "http://10.120.111.143:8106/sa?project=default";
//        DebugConsumer debugConsumer = new DebugConsumer(url, true);
//        saTmp = new SensorsAnalytics(debugConsumer);

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

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    }

    @Test
    public void testInvalidTrackId() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111.123);

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals(111.123, messageList.get(0).get("_track_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    }

    @Test
    public void testInvalidTrackId01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", "abc");

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals("abc", messageList.get(0).get("_track_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    }

    @Test
    @Ignore
    public void testInvalidTrackId02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", null);

        saTmp.track("xccc", true, "test1", properties);
//        saTmp.trackById(identity, "test", properties);
        saTmp.trackById(identity, "test",null);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals("abc", messageList.get(0).get("_track_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    }

    @Test
    @Ignore
    public void testInvalidTime() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        Date date = new Date();
        properties.put("$time", date.getTime());

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals(date.getTime(), messageList.get(0).get("time"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$time")); // properties 不包含 $track_id
    }

    @Test
    @Ignore
    public void testInvalidTime01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$time", "abc");

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals("abc", messageList.get(0).get("time"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$time")); // properties 不包含 $track_id
    }

    @Test
    @Ignore
    public void testInvalidTime02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$time", null);

        saTmp.trackById(identity, "test", properties);
        saTmp.trackById(identity, "test",null);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals("abc", messageList.get(0).get("time"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$time")); // properties 不包含 $track_id
    }

    @Test
    public void testInvalidTime03() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$time", new Date(1671828405241L));

        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals("abc", messageList.get(0).get("time"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$time")); // properties 不包含 $track_id
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
     * 校验 ID-Mapping 公共属性
     */
    @Test
    public void checkTrackByIdSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);

        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性
    }

    /**
     * 校验 ID-Mapping 公共属性
     */
    @Test
    public void checkTrackByIdSuperProperties01() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", "aaa");
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);

        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertNotEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("abc")); // properties 包含其他自定义属性
    }


    /**
     * 校验 ID-Mapping bind 接口公共属性
     */
    @Test
    public void checkBindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
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
     * 校验 ID_Mapping unbind 接口公共属性
     */
    @Test
    public void checkUnbindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
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
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .setDistinctId("disId123")
                .setEventName("test")
                .addProperty("$track_id", 111)
                .addProperty("abc", 111)
                .build();

        saTmp.trackById(record);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

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

        saTmp.profileSetById(identity, properties);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

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
        saTmp.profileSetOnceById(identity, properties);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementById00() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        properties.put("$track_id", 111);

        saTmp.profileIncrementById(identity, properties);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    @Test
    public void testProfileAppendById() {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        properties.put("$track_id", 111);
        try {
            saTmp.profileAppendById(identity, properties);
            fail("profileAppendById should throw InvalidArgumentException.");
        }catch (InvalidArgumentException e){
            assertEquals("The property value of PROFILE_APPEND should be a List<String>.", e.getMessage());
            e.printStackTrace();
        }
    }

    @Test
    public void testProfileUnsetById() {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        properties.put("$track_id", 111);

        try {
            saTmp.profileUnsetById(identity, properties);
            fail("profileUnsetById should throw InvalidArgumentException.");
        }catch (InvalidArgumentException e){
            assertEquals("The property value of [$track_id] should be true.", e.getMessage());
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
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111) // 设置 $track_id
                .build();
        saTmp.profileSetById(userRecord);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

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
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111) // 设置 $track_id
                .build();
        saTmp.profileSetOnceById(userRecord);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew() throws InvalidArgumentException {
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111) // 设置 $track_id
                .build();
        saTmp.profileIncrementById(userRecord);

        assertEquals(1, messageList.size());
        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id")); // 公共属性设置 $track_id 不生效的

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
        assertTrue(props.containsKey("age")); // properties 包含其他自定义属性
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

        try {
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", list) // 设置埋点事件属性
                .addProperty("$track_id", 111) // 设置 $track_id
                .build();

            saTmp.profileAppendById(userRecord);
            fail("profileAppendById should throw InvalidArgumentException.");
        }catch (InvalidArgumentException e){
            assertEquals("The property value of PROFILE_APPEND should be a List<String>.", e.getMessage());
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
        IDMUserRecord userRecord = null;
        try {
            userRecord = IDMUserRecord.starter()
                    .setDistinctId("xc001") //手动指定外层 distinct_id
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                    .addProperty("favorite", true) // 设置埋点事件属性
                    .addProperty("$track_id", 111) // 设置 $track_id
                    .build();

            saTmp.profileUnsetById(userRecord);
            fail("profileUnsetById should throw InvalidArgumentException.");
        }catch (InvalidArgumentException e){
            assertEquals("The property value of $track_id should be true.", e.getMessage());
            e.printStackTrace();
        }
    }

}
