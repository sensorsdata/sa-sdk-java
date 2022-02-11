package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class IDMappingModel3TestBind extends SensorsBaseTest {

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
        batchConsumer = new BatchConsumer(url, 100, true, 3);
        Field field = batchConsumer.getClass().getDeclaredField("messageList");
        field.setAccessible(true);
        messageList = (List<Map<String, Object>>) field.get(batchConsumer);
        saTmp = new SensorsAnalytics(batchConsumer);
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

    @Test
    public void TestBindInvalidIdentityNull() throws InvalidArgumentException{
        try{
            saTmp.bind(null);
        }catch (NullPointerException e){
            e.printStackTrace();
            assertEquals("identities is marked non-null but is null", e.getMessage());
        }

    }

    @Test
    public void TestBindInvalidIdentityEmpty() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The identities is invalid，you should have at least two identities.", e.getMessage());
        }

        saTmp.flush();
    }

    @Test
    public void TestBindInvalidIdentityKey01() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(null, "123")
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The track_id_bind key is empty or null.", e.getMessage());
        }
    }

    @Test
    public void TestBindInvalidIdentityKey02() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty("", "123")
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The track_id_bind key is empty or null.", e.getMessage());
        }
    }

    @Test
    public void TestBindInvalidIdentityKey03() throws InvalidArgumentException{
        String keys[] = {"123id", "用户名", "abc@#%^&*", "date", "datetime", "distinct_id", "event", "events", "first_id", "id", "original_id",
                "device_id", "properties", "second_id", "time", "user_id", "users", "user_group123", "user_tag456"};

        for (int i = 0; i < keys.length; i++){
            try {
                SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(keys[i], "123")
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                        .build();
                saTmp.bind(identity);
            }catch (InvalidArgumentException e){
                System.out.println(e.getMessage());
                assertEquals(String.format("The %s key '%s' is invalid.", "track_id_bind", keys[i]), e.getMessage());
            }
        }
    }

    @Test
    public void TestBindInvalidIdentityKey04() throws InvalidArgumentException{
        String key = "";
        for(int i = 0; i < 100; i++){
            key = key + i;
        }
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(key, "123")
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s key '%s' is invalid.", "track_id_bind", key), e.getMessage());
        }
    }

    @Test
    public void TestBindInvalidIdentityValue01() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, null)
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s value is empty or null.", "track_id_bind"), e.getMessage());
        }
    }

    @Test
    public void TestBindInvalidIdentityValue02() throws InvalidArgumentException{
        String value = "";
        for(int i = 0; i < 255; i++){
            value = value + i;
        }
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, value)
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s value %s is too long, max length is 255.", "track_id_bind", value), e.getMessage());
        }
    }

    @Test
    public void TestBindInvalidIdentityValue03() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "")
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
                    .build();
            saTmp.bind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s value is empty or null.", "track_id_bind"), e.getMessage());
        }
    }

    /**
     * 校验 ID-Mapping unbind 接口用户格式
     */
    @Test
    public void TestUnbindUserId() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.unbind(identity);
        assertNotNull(messageList.get(0).get("identities"));
        assertTrue(messageList.get(0).get("identities") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("identities");
        assertEquals("id_value1", result.get("id_test1"));
        assertEquals("track_id_unbind", messageList.get(0).get("type"));
    }

    /**
     * 校验 ID-Mapping unbind 接口用户格式
     */
    @Test
    public void TestUnbindUserId01() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .addIdentityProperty("$anonymous_id", "anonymous_id_value1")
                .addIdentityProperty("$login_id", "login_id_value1")
                .build();
        saTmp.unbind(identity);
        assertNotNull(messageList.get(0).get("identities"));
        assertTrue(messageList.get(0).get("identities") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("identities");
        assertEquals("id_value1", result.get("id_test1"));
        assertEquals("track_id_unbind", messageList.get(0).get("type"));
    }

    @Test
    public void TestUnBindInvalidIdentityNull() throws InvalidArgumentException{
        try{
            saTmp.unbind(null);
        }catch (NullPointerException e){
            e.printStackTrace();
            assertEquals("analyticsIdentity is marked non-null but is null", e.getMessage());
        }

    }

    @Test
    public void TestUnBindInvalidIdentityEmpty() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The identity is empty.", e.getMessage());
        }

        saTmp.flush();
    }

    @Test
    public void TestUnBindInvalidIdentityKey01() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(null, "123")
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The track_id_unbind key is empty or null.", e.getMessage());
        }
    }

    @Test
    public void TestUnBindInvalidIdentityKey02() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty("", "123")
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The track_id_unbind key is empty or null.", e.getMessage());
        }
    }

    @Test
    public void TestUnBindInvalidIdentityKey03() throws InvalidArgumentException{
        String keys[] = {"123id", "用户名", "abc@#%^&*", "date", "datetime", "distinct_id", "event", "events", "first_id", "id", "original_id",
                "device_id", "properties", "second_id", "time", "user_id", "users", "user_group123", "user_tag456"};

        for (int i = 0; i < keys.length; i++){
            try {
                SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(keys[i], "123")
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                        .build();
                saTmp.unbind(identity);
            }catch (InvalidArgumentException e){
                System.out.println(e.getMessage());
                assertEquals(String.format("The %s key '%s' is invalid.", "track_id_unbind", keys[i]), e.getMessage());
            }
        }
    }

    @Test
    public void TestUnBindInvalidIdentityKey04() throws InvalidArgumentException{
        String key = "";
        for(int i = 0; i < 100; i++){
            key = key + i;
        }
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(key, "123")
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s key '%s' is invalid.", "track_id_unbind", key), e.getMessage());
        }
    }

    @Test
    public void TestUnBindInvalidIdentityValue01() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, null)
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s value is empty or null.", "track_id_unbind"), e.getMessage());
        }
    }

    @Test
    public void TestUnBindInvalidIdentityValue02() throws InvalidArgumentException{
        String value = "";
        for(int i = 0; i < 255; i++){
            value = value + i;
        }
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, value)
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s value %s is too long, max length is 255.", "track_id_unbind", value), e.getMessage());
        }
    }

    @Test
    public void TestUnBindInvalidIdentityValue03() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "")
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
                    .build();
            saTmp.unbind(identity);
        }catch (InvalidArgumentException e){
            System.out.println(e.getMessage());
            assertEquals(String.format("The %s value is empty or null.", "track_id_unbind"), e.getMessage());
        }
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
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "id_value1")
                .build();
        saTmp.unbind(identity);
        assertTrue(messageList.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }

    /**
     * 校验 ID-Mapping 公共属性
     */
    @Test
    public void TestTrackByIdSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        saTmp.registerSuperProperties(properties);
        saTmp.clearSuperProperties();
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);
        assertTrue(messageList.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }

    /**
     * 校验 ID-Mapping bind 接口公共属性
     */
    @Test
    public void TestBindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        saTmp.registerSuperProperties(properties);
        saTmp.clearSuperProperties();
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .addIdentityProperty("eee", "123")
                .build();
        saTmp.bind(identity);
        assertTrue(messageList.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }

    /**
     * 校验 ID_Mapping unbind 接口公共属性
     */
    @Test
    public void TestUnbindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "id_value1")
                .build();
        saTmp.unbind(identity);
        assertTrue(messageList.get(0).get("properties") instanceof Map);
        Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("properties");
        assertEquals("123", result.get("asd"));
    }
}
