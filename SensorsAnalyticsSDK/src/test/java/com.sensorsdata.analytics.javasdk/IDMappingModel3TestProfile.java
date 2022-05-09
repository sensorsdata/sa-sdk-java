package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 *  适用于 v3.4.2+ 版本
 */
public class IDMappingModel3TestProfile extends SensorsBaseTest {

    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    private ConcurrentLoggingConsumer consumer;

    SensorsAnalytics saTmp;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
        initBatchConsumer();
    }

    public void initBatchConsumer() throws NoSuchFieldException, IllegalAccessException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        batchConsumer = new BatchConsumer(url, 100, true, 3);
        Field field = batchConsumer.getClass().getDeclaredField("messageList");
        field.setAccessible(true);
        messageList = (List<Map<String, Object>>) field.get(batchConsumer);
        saTmp = new SensorsAnalytics(batchConsumer);
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * 测试点：SensorsAnalyticsIdentity 传入 $identity_login_id 和其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetById01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetById02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnesById00() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetOnceById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * 测试点：SensorsAnalyticsIdentity 传入 $identity_login_id 和其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceById01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetOnceById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceById02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetOnceById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        saTmp.flush();
    }





    @Test
    public void testProfileSetOnceById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetOnceById(identity,properties);
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set_once", messageList.get(0).get("type"));
        assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementById00() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileIncrementById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * 测试点：SensorsAnalyticsIdentity 传入 $identity_login_id 和其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementById01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileIncrementById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementById02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileIncrementById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        saTmp.flush();
    }

    @Test
    public void testProfileIncrementById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileIncrementById(identity,properties);
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_increment", messageList.get(0).get("type"));
        // v3.4.2 新增逻辑：distinct_id 取第一个维度标识作为 distinct_id，且取值格式为 key+value；
        assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    @Test
    public void testProfileAppendById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity,properties);
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        // v3.4.2 新增逻辑：distinct_id 取第一个维度标识作为 distinct_id，且取值格式为 key+value；
        assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendById00() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * 测试点：SensorsAnalyticsIdentity 传入 $identity_login_id 和其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendById01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendById02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        saTmp.flush();
    }

    // profileUnsetById
    @Test
    public void testProfileUnsetById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileUnsetById(identity,properties);
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertFalse(props.containsKey("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_unset", messageList.get(0).get("type"));
        // v3.4.2 新增逻辑：distinct_id 取第一个维度标识作为 distinct_id，且取值格式为 key+value；
        assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetById00() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();

        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileUnsetById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * 测试点：SensorsAnalyticsIdentity 传入 $identity_login_id 和其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetById01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileUnsetById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetById02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();

        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileUnsetById(identity,properties);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        saTmp.flush();
    }

    // profileDeleteById
    @Test
    public void testProfileDeleteById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();

        saTmp.profileDeleteById(identity);
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_delete", messageList.get(0).get("type"));
        // v3.4.2 新增逻辑：distinct_id 取第一个维度标识作为 distinct_id，且取值格式为 key+value；
        assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }
    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileDeleteById00() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();

        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileDeleteById(identity);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


    /**
     * 测试点：SensorsAnalyticsIdentity 传入 $identity_login_id 和其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileDeleteById01() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileDeleteById(identity);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_login_id"));
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("$is_login_id"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    /**
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileDeleteById02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .build();

        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileDeleteById(identity);
        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("13800000001", identities.get("$identity_mobile"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_mobile+13800000001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        saTmp.flush();
    }


    /**
     *  v3.4.2 新增接口 trackById(@NonNull IDMEventRecord idmEventRecord)
     */

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNew() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        saTmp.flush();
    }


    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNew01() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNew02() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertEquals("dis123", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNew03() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_email+123@qq.com", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNewInvalid01() throws InvalidArgumentException{
        try {
            saTmp.profileSetById(null);
            fail("[ERROR] should throw NullPointerException");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNew() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetOnceById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set_once", messageList.get(0).get("type"));
        saTmp.flush();
    }


    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNew01() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetOnceById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set_once", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNew02() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetOnceById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertEquals("dis123", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set_once", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNew03() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetOnceById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_email+123@qq.com", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set_once", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNewInvalid01() throws InvalidArgumentException{
        try {
            saTmp.profileSetById(null);
            fail("[ERROR] should throw NullPointerException");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileIncrementById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_increment", messageList.get(0).get("type"));
        saTmp.flush();
    }


    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew01() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileIncrementById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_increment", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew02() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileIncrementById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(true, props.get("$is_login_id"));
        assertEquals("dis123", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_increment", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew03() throws InvalidArgumentException{
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileIncrementById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_email+123@qq.com", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_increment", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNewInvalid01() throws InvalidArgumentException{
        try {
            saTmp.profileSetById(null);
            fail("[ERROR] should throw NullPointerException");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendByIdNew() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", list) // 设置埋点事件属性
                .build();
        saTmp.profileAppendById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        saTmp.flush();
    }


    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendByIdNew01() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", list) // 设置埋点事件属性
                .build();
        saTmp.profileAppendById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendByIdNew02() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", list) // 设置埋点事件属性
                .build();
        saTmp.profileAppendById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertEquals(true, props.get("$is_login_id"));
        assertEquals("dis123", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendByIdNew03() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", list) // 设置埋点事件属性
                .build();
        saTmp.profileAppendById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_email+123@qq.com", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetAppendByIdNewInvalid01() throws InvalidArgumentException{
        try {
            saTmp.profileAppendById(null);
            fail("[ERROR] should throw NullPointerException");
        }catch (Exception e){
            e.printStackTrace();
        }
    }


    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNew() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", true) // 设置埋点事件属性
                .build();
        saTmp.profileUnsetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_unset", messageList.get(0).get("type"));
        saTmp.flush();
    }


    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入其他用户标识
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNew01() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", true) // 设置埋点事件属性
                .build();
        saTmp.profileUnsetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("xc001", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_unset", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNew02() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", true) // 设置埋点事件属性
                .build();
        saTmp.profileUnsetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("dis123", identities.get("$identity_login_id"));
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("favorite"));
        assertEquals(true, props.get("$is_login_id"));
        assertEquals("dis123", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_unset", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNew03() throws InvalidArgumentException{
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", true) // 设置埋点事件属性
                .build();
        saTmp.profileUnsetById(userRecord);

        assertEquals(1, messageList.size());

        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123@qq.com", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("favorite"));
        assertFalse(props.containsKey("$is_login_id"));
        assertEquals("$identity_email+123@qq.com", messageList.get(0).get("distinct_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_unset", messageList.get(0).get("type"));
        saTmp.flush();
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNewInvalid01() throws InvalidArgumentException{
        try {
            saTmp.profileUnsetById(null);
            fail("[ERROR] should throw NullPointerException");
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testIDMUserRecordInvalid01() throws InvalidArgumentException{
        try {
            // 新版本接口
            IDMUserRecord userRecord = IDMUserRecord.starter()
                    .setDistinctId(null)
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")   //用户维度标识
                    .addProperty("favorite", true) // 设置埋点事件属性
                    .build();
            fail("[ERROR] should throw NullPointerException");
            saTmp.profileUnsetById(userRecord);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testIDMUserRecordInvalid02() throws InvalidArgumentException{
        try {
            // 新版本接口
            IDMUserRecord userRecord = IDMUserRecord.starter()
                    .setDistinctId(null)
                    .addProperty("favorite", true) // 设置埋点事件属性
                    .build();
            fail("[ERROR] should throw NullPointerException");
            saTmp.profileUnsetById(userRecord);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testIDMUserRecordInvalid03() throws InvalidArgumentException{
        try {
            // 新版本接口
            IDMUserRecord userRecord = IDMUserRecord.starter()
                    .setDistinctId("")
                    .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")   //用户维度标识
                    .addProperty("favorite", true) // 设置埋点事件属性
                    .build();
            fail("[ERROR] should throw NullPointerException");
            saTmp.profileUnsetById(userRecord);
        }catch (Exception e){
            e.printStackTrace();
        }
    }

    /**
     * IDMUserRecord 传入 null
     * @throws InvalidArgumentException
     */
    @Test
    public void testIDMUserRecordInvalid04() throws InvalidArgumentException{
        try {
            // 新版本接口
            IDMUserRecord userRecord = IDMUserRecord.starter()
                    .setDistinctId("")
                    .addProperty("favorite", true) // 设置埋点事件属性
                    .build();
            fail("[ERROR] should throw NullPointerException");
            saTmp.profileUnsetById(userRecord);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
