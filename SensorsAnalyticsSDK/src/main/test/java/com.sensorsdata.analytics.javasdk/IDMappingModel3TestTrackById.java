package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;



public class IDMappingModel3TestTrackById extends SensorsBaseTest {

    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    private ConcurrentLoggingConsumer consumer;

    private StringBuilder messageBuffer;
    SensorsAnalytics saTmp;

//    @Before
//    public void init() throws NoSuchFieldException, IllegalAccessException {
//        consumer = new ConcurrentLoggingConsumer("file.log");
//        Field field = consumer.getClass().getSuperclass().getDeclaredField("messageBuffer");
//        field.setAccessible(true);
//        messageBuffer = (StringBuilder) field.get(consumer);
//        saTmp = new SensorsAnalytics(consumer);
//    }

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
//        "\"http://10.120.235.239:8106/sa?project=default\""
        batchConsumer = new BatchConsumer(url, 100, 3, true);
        Field field = batchConsumer.getClass().getDeclaredField("messageList");
        field.setAccessible(true);
        messageList = (List<Map<String, Object>>) field.get(batchConsumer);
        saTmp = new SensorsAnalytics(batchConsumer);
    }

    @Test
    public void TestSingleIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
//        properties.put("test", "test");
//        properties.put("$project", "abc");
//        properties.put("$token", "123");
        saTmp.trackById(identity, "test", properties);
        saTmp.bind(identity,identity,identity);
//        System.out.println(messageList.isEmpty());
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("test", messageList.get(0).get("event"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        assertNotNull(messageList.get(0).get("properties"));
        assertNotNull(messageList.get(0).get("project"));
        assertNotNull(messageList.get(0).get("token"));
        saTmp.flush();
    }

    @Test
    public void TestTrackByIdMultiIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .build();
        Map<String, Object> properties = new HashMap<>();
//        properties.put("test", "test");
//        properties.put("$project", "abc");
//        properties.put("$token", "123");
        saTmp.trackById(identity, "TestMultiIdentity", properties);
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("TestMultiIdentity", messageList.get(0).get("event"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        assertNotNull(messageList.get(0).get("properties"));
        assertNotNull(messageList.get(0).get("project"));
        assertNotNull(messageList.get(0).get("token"));
        saTmp.flush();
    }

    @Test
    public void TestTrackByIdPropertiesNull() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .build();
        Map<String, Object> properties = null;
//        properties.put("test", "test");
//        properties.put("$project", "abc");
//        properties.put("$token", "123");
        saTmp.trackById(null, "TestPropertiesNull", properties);
        assertFalse(messageList.isEmpty());

        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));
        assertEquals("email1@qq.com", identities.get("$identity_email"));

        saTmp.flush();
    }

    @Test
    public void TestTrackByIdInvalidIdentityNull() throws InvalidArgumentException{
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", "test");
        properties.put("$project", "abc");
        properties.put("$token", "123");
        saTmp.trackById(null, "TestPropertiesNull", properties);
    }

    @Test
    public void TestTrackByIdInvalidIdentityEmpty() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", "test");
        properties.put("$project", "abc");
        properties.put("$token", "123");
        saTmp.trackById(identity, "test", properties);
        saTmp.flush();
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey01() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(null, "123")
                .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s key is empty or null.", "track"), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey02() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty("", "123")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s key is empty or null.", "track"), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey03() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty("123id", "123")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals("The track key '123id' is invalid.", e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey04() throws InvalidArgumentException{
        String key = "用户名";
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(key, "123")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s key '%s' is invalid.", "track", key), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey05() throws InvalidArgumentException{
        String key = "abc@#%^&*";
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(key, "123")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s key '%s' is invalid.", "track", key), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey06() throws InvalidArgumentException{
        String keys[] = {"date", "datetime", "distinct_id", "event", "events", "first_id", "id", "original_id",
                "device_id", "properties", "second_id", "time", "user_id", "users", "user_group123", "user_tag456"};

        for (int i = 0; i < keys.length; i++){
            try {
                SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(keys[i], "123")
                        .build();
                Map<String, Object> properties = new HashMap<>();
                properties.put("test", "test");
                properties.put("$project", "abc");
                properties.put("$token", "123");
                saTmp.trackById(identity, "test", properties);
            }catch (InvalidArgumentException e){
                System.out.println(e.getMessage());
                assertEquals(String.format("The %s key '%s' is invalid.", "track", keys[i]), e.getMessage());
            }
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityKey07() throws InvalidArgumentException{
        String key = "";
        for(int i = 0; i < 100; i++){
            key = key + i;
        }
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(key, "123")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s key '%s' is invalid.", "track", key), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityValue01() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, null)
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s value is empty or null.", "track"), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityValue03() throws InvalidArgumentException{
        String value = "";
        for(int i = 0; i < 255; i++){
            value = value + i;
        }
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, value)
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s value %s is too long, max length is 255.", "track", value), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityValue02() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "")
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
        }catch (InvalidArgumentException e){
            assertEquals(String.format("The %s value is empty or null.", "track"), e.getMessage());
        }
    }

    @Test
    public void TestTrackByIdInvalidIdentityEventName01() throws InvalidArgumentException{
        // 创建集合
        ArrayList<String> eventNames = new ArrayList<String>();
        eventNames.add("");

        // 获取迭代器
        Iterator<String> it = eventNames.iterator();
        while(it.hasNext()) {
            try {
                SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123456")
                        .build();
                Map<String, Object> properties = new HashMap<>();
                properties.put("test", "test");
                properties.put("$project", "abc");
                properties.put("$token", "123");
                saTmp.trackById(identity, it.next(), properties);
            } catch (InvalidArgumentException e) {
                assertEquals(String.format("The %s is empty or null.", "event name key"), e.getMessage());
            } catch (NullPointerException e){
                assertEquals("eventName is marked non-null but is null", e.getMessage());
            }
        }
    }

    @Test
    public void TestTrackByIdInvalidEventName02() throws InvalidArgumentException{
        String eventNames[] = {"date", "datetime", "distinct_id", "event", "events", "first_id", "id", "original_id",
                "device_id", "properties", "second_id", "time", "user_id", "users", "user_group123", "user_tag456"};

        for (int i = 0; i < eventNames.length; i++){
            try {
                SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                        .build();
                Map<String, Object> properties = new HashMap<>();
                properties.put("test", "test");
                properties.put("$project", "abc");
                properties.put("$token", "123");
                saTmp.trackById(identity, eventNames[i], properties);
            }catch (InvalidArgumentException e){
                System.out.println(e.getMessage());
                assertEquals(String.format("The %s key '%s' is invalid.", "event name", eventNames[i]), e.getMessage());
            }
        }
    }

    /**
     * 校验 ID-Mapping bind 接口
     */
    @Test
    public void TestIdMappingBind() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("$identity_mobile", "123")
                .addIdentityProperty("$identity_email", "fz@163.com")
                .build();
        saTmp.bind(identity);
        assertNotNull(messageList.get(0).get("identities"));
        assertTrue(messageList.get(0).get("identities") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("identities");
        assertEquals(2, result.size());
        assertEquals("123", result.get("$identity_mobile"));
        assertEquals("fz@163.com", result.get("$identity_email"));
        assertEquals("$BindID", messageList.get(0).get("event"));
        assertEquals("track_id_bind", messageList.get(0).get("type"));
    }

    /**
     * 校验 ID—Mapping bind 接口单用户属性
     */
    @Test
    public void TestIdMappingBindOneId() {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("$identity_mobile", "123")
                .build();
        try {
            saTmp.bind(identity);
        } catch (InvalidArgumentException e) {
            System.out.println(e.getMessage());
            assertEquals("The identities is invalid，you should have at least two identities.", e.getMessage());
        }
    }

    /**
     * 校验 ID-Mapping unbind 接口用户格式
     */
    @Test
    public void checkUnbindUserId() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        sa.unbind(identity);
        assertNotNull(res.get(0).get("identities"));
        assertTrue(res.get(0).get("identities") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) res.get(0).get("identities");
        assertEquals("id_value1", result.get("id_test1"));
        assertEquals("track_id_unbind", res.get(0).get("type"));
    }

    /**
     * 校验 ID-Mapping 公共属性
     */
    @Test
    public void checkTrackByIdSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        sa.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        sa.trackById(identity, "eee", null);
        assertTrue(res.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }

    /**
     * 校验 ID-Mapping bind 接口公共属性
     */
    @Test
    public void checkBindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        sa.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .addIdentityProperty("eee", "123")
                .build();
        sa.bind(identity);
        assertTrue(res.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }

    /**
     * 校验 ID_Mapping unbind 接口公共属性
     */
    @Test
    public void checkUnbindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        sa.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        sa.unbind(identity);
        assertTrue(res.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }
}
