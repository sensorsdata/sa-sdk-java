package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;

/**
 *  适用于 v3.4.2+ 版本
 */
public class IDMappingModel3TestTrackById extends SensorsBaseTest {
    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    SensorsAnalytics saTmp;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
//        String url = "http://10.120.73.51:8106/sa?project=default&token=";
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
//        assertNotNull(messageList.get(0).get("project"));
//        assertNotNull(messageList.get(0).get("token"));
    }

    @Test
    public void testSingleIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", "test");
        properties.put("$project", "abc");
        properties.put("$token", "123");
        saTmp.trackById(identity, "test", properties);
        assertEquals(1, messageList.size());
        assertNotNullProp();

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertTrue(isLoginID);

        assertEquals("123", messageList.get(0).get("distinct_id"));
        assertEquals("test", messageList.get(0).get("event"));
        saTmp.flush();
    }

    @Test
    public void testTrackByIdMultiIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", "test");
        properties.put("$project", "abc");
        properties.put("$token", "123");
        saTmp.trackById(identity, "testMultiIdentity", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertEquals("testMultiIdentity", messageList.get(0).get("event"));
        assertEquals("123", messageList.get(0).get("distinct_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertTrue(isLoginID);

        saTmp.flush();
    }

    @Test
    public void testTrackByIdMultiIdentity02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .addIdentityProperty("MyID", "001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", "test");
        properties.put("$project", "abc");
        properties.put("$token", "123");

        saTmp.trackById(identity, "testMultiIdentity", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertEquals("testMultiIdentity", messageList.get(0).get("event"));

        // v3.4.2 新增逻辑：distinct_id 取第一个维度标识作为 distinct_id，且取值格式为 key+value；
        // 为："$identity_mobile+13800000001"
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertFalse(isLoginID);

        saTmp.flush();
    }


    @Test
    public void testTrackByIdPropertiesNull() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .build();
        Map<String, Object> properties = null;
        saTmp.trackById(identity, "testPropertiesNull", properties);
        assertFalse(messageList.isEmpty());

        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));
        assertEquals("email1@qq.com", identities.get("$identity_email"));

        saTmp.flush();
    }

    @Test
    public void testTrackByIdInvalidIdentityNull() throws InvalidArgumentException{
        try {
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(null, "testPropertiesNull", properties);
            fail("[ERROR] should throw NullPointerException");
        }catch (Exception e){
            assertEquals("analyticsIdentity is marked non-null but is null", e.getMessage());
        }
    }

    @Test
    public void testTrackByIdInvalidIdentityEmpty() throws InvalidArgumentException{
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .build();
            Map<String, Object> properties = new HashMap<>();
            properties.put("test", "test");
            properties.put("$project", "abc");
            properties.put("$token", "123");
            saTmp.trackById(identity, "test", properties);
            saTmp.flush();
        }catch (Exception e){
            assertEquals("The identity is empty.", e.getMessage());
        }
    }

    @Test
    public void testTrackByIdInvalidIdentityKey01() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityKey02() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityKey03() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityKey04() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityKey05() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityKey06() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityKey07() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityValue01() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityValue03() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityValue02() throws InvalidArgumentException{
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
    public void testTrackByIdInvalidIdentityEventName01() throws InvalidArgumentException{
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
                assertEquals(String.format("The %s is empty or null.", "event_name key"), e.getMessage());
            } catch (NullPointerException e){
                assertEquals("eventName is marked non-null but is null", e.getMessage());
            }
        }
    }

    @Test
    public void testTrackByIdInvalidEventName02() throws InvalidArgumentException{
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
                assertEquals(String.format("The %s key '%s' is invalid.", "event_name", eventNames[i]), e.getMessage());
            }
        }
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
    public void testIdMappingBindOneId() {
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
                .addProperty("test", "test")
                .addProperty("$project", "default")
                .addProperty("$token", "123")
                .build();


        saTmp.trackById(record);
        assertEquals(1, messageList.size());
        assertNotNullProp();

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertFalse(isLoginID);

        assertEquals("disId123", messageList.get(0).get("distinct_id"));
        assertEquals("test", messageList.get(0).get("event"));
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入正常 distinctId，identityMap 传入其他用户标识
     */
    @Test
    public void testTrackByIdIDMEventRecord01() throws InvalidArgumentException {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .setDistinctId("disId123")
                .setEventName("test")
                .addProperty("test", "test")
                .addProperty("$project", "default")
                .addProperty("$token", "123")
                .build();

        saTmp.trackById(record);
        assertEquals(1, messageList.size());
        assertNotNullProp();

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertFalse(isLoginID);

        assertEquals("disId123", messageList.get(0).get("distinct_id"));
        assertEquals("test", messageList.get(0).get("event"));
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     */

    @Test
    public void testTrackByIdIDMEventRecord02() throws InvalidArgumentException {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "xc001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .setEventName("test")
                .addProperty("test", "test")
                .addProperty("$project", "default")
                .addProperty("$token", "123")
                .build();

        saTmp.trackById(record);
        assertEquals(1, messageList.size());
        assertNotNullProp();

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertTrue(isLoginID);

        assertEquals("xc001", messageList.get(0).get("distinct_id"));
        assertEquals("test", messageList.get(0).get("event"));
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 不传入 distinctId，identityMap 传入其他用户标识
     */

    @Test
    public void testTrackByIdIDMEventRecord03() throws InvalidArgumentException {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "1300000055")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .setEventName("test")
                .addProperty("test", "test")
                .addProperty("$project", "default")
                .addProperty("$token", "123")
                .build();

        saTmp.trackById(record);
        assertEquals(1, messageList.size());
        assertNotNullProp();

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        Boolean isLoginID = (Boolean)props.get("$is_login_id");
        assertFalse(isLoginID);

        assertEquals("$identity_mobile+1300000055", messageList.get(0).get("distinct_id"));
        assertEquals("test", messageList.get(0).get("event"));
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 不传入 distinctId，identityMap 传入其他用户标识
     */

    @Test
    public void testTrackByIdIDMEventRecord04() throws InvalidArgumentException {
        try {
            IDMEventRecord record = IDMEventRecord.starter()
                    .setDistinctId("dis123")
                    .setEventName("test")
                    .addProperty("test", "test")
                    .addProperty("$project", "default")
                    .addProperty("$token", "123")
                    .build();

            fail("[ERROR] should throw InvalidArgumentException");
            saTmp.trackById(record);
        }catch(InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The identity is empty.", e.getMessage());
        }

    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入异常 distinctId = null
     */
    @Test
    public void testTrackByIdIDMEventRecordInvalid01() throws InvalidArgumentException{
        try {
            IDMEventRecord record = IDMEventRecord.starter()
                    .setDistinctId(null)
                    .setEventName("test")
                    .addProperty("test", "test")
                    .addProperty("$project", "default")
                    .addProperty("$token", "123")
                    .build();

            fail("[ERROR] should throw InvalidArgumentException");
            saTmp.trackById(record);
        }catch(NullPointerException e){
            e.printStackTrace();
            assertEquals("distinctId is marked non-null but is null", e.getMessage());
        }
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入异常 distinctId = null
     */
    @Test
    public void testTrackByIdIDMEventRecordInvalid02() throws InvalidArgumentException {
        try {
            IDMEventRecord record = IDMEventRecord.starter()
                    .setDistinctId(null)
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "1300000055")
                    .setEventName("test")
                    .addProperty("test", "test")
                    .addProperty("$project", "default")
                    .addProperty("$token", "123")
                    .build();

            saTmp.trackById(record);
            fail("[ERROR] should throw InvalidArgumentException");
        }catch(NullPointerException e){
            e.printStackTrace();
            assertEquals("distinctId is marked non-null but is null", e.getMessage());
        }
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入异常 distinctId = ""
     */
    @Test
    public void testTrackByIdIDMEventRecordInvalid03() {
        try {
            IDMEventRecord record = IDMEventRecord.starter()
                    .setDistinctId("")
                    .setEventName("test")
                    .addProperty("test", "test")
                    .addProperty("$project", "default")
                    .addProperty("$token", "123")
                    .build();

            saTmp.trackById(record);
            fail("[ERROR] should throw InvalidArgumentException");
        }catch(InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The identity is empty.", e.getMessage());
        }
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入异常 distinctId = ""
     */
    @Test
    public void testTrackByIdIDMEventRecordInvalid04()  {
        try {
            IDMEventRecord record = IDMEventRecord.starter()
                    .setDistinctId("")
                    .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "1300000055")
                    .setEventName("test")
                    .addProperty("test", "test")
                    .addProperty("$project", "default")
                    .addProperty("$token", "123")
                    .build();

            saTmp.trackById(record);
            fail("[ERROR] should throw InvalidArgumentException");
        }catch(InvalidArgumentException e){
            e.printStackTrace();
            assertEquals("The distinct_id value is empty or null.", e.getMessage());
        }
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 整体为 null
     */
    @Test
    public void testTrackByIdIDMEventRecordInvalid05()  {
        try {
            saTmp.trackById(null);
            fail("[ERROR] should throw NullPointerException");
        }catch(Exception e){
            e.printStackTrace();
            assertEquals("idmEventRecord is marked non-null but is null", e.getMessage());
        }
    }

    /**
     *  v3.4.4 新增功能： properties 可传入的 $track_id
     */
    @Test
    public void testTrackId() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);

        saTmp.trackById(identity, "testMultiIdentity", properties);
        assertEquals(1, messageList.size());

        assertNotNullProp();

        assertEquals(111, messageList.get(0).get("_track_id"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    }

}
