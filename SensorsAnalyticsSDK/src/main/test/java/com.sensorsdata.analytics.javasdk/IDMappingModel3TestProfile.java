package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;


public class IDMappingModel3TestProfile extends SensorsBaseTest {

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
    public void TestProfileSetById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetById(identity,properties);
//        System.out.println(messageList.isEmpty());
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
        assertNotNull(messageList.get(0).get("project"));
        saTmp.flush();
    }

    @Test
    public void TestProfileSetOnceById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetOnceById(identity,properties);
//        System.out.println(messageList.isEmpty());
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(false, props.get("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_set_once", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    @Test
    public void TestProfileIncrementById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileIncrementById(identity,properties);
//        System.out.println(messageList.isEmpty());
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(1, props.get("age"));
        assertEquals(false, props.get("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_increment", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    @Test
    public void TestProfileAppendById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity,properties);
//        System.out.println(messageList.isEmpty());
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertEquals(false, props.get("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    // profileUnsetById
    @Test
    public void TestProfileUnsetById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileUnsetById(identity,properties);
//        System.out.println(messageList.isEmpty());
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(true, props.get("favorite"));
        assertEquals(false, props.get("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_unset", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }

    // profileDeleteById
    @Test
    public void TestProfileDeleteById() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity,properties);
//        System.out.println(messageList.isEmpty());
        assertEquals(1, messageList.size());
        assertNotNull(messageList.get(0).get("identities"));
        Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
        assertEquals("123", identities.get("$identity_email"));

        Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
        assertEquals(list, props.get("favorite"));
        assertEquals(false, props.get("$is_login_id"));

        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertEquals("profile_append", messageList.get(0).get("type"));
        assertEquals("123", messageList.get(0).get("distinct_id"));
        saTmp.flush();
    }


}
