package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 工具类测试
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/25 14:03
 */
public class SensorsAnalyticsUtilTest {

    @Test
    public void checkAssertFailedDataWithNoTrackId() {
        Map<String, Object> event = new HashMap<>();
        event.put("distinct_id", "12345");
        event.put("event", "test");
        event.put("type", "track");
        Map<Object, Object> properties = new HashMap<>();
        properties.put("test", "test");
        event.put("properties", properties);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        FailedData failedData = new FailedData("", list);
        try {
            SensorsAnalyticsUtil.assertFailedData(failedData);
            Assertions.fail();
        } catch (InvalidArgumentException e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void checkAssertFailedDataWithNoType() {
        Map<String, Object> event = new HashMap<>();
        event.put("_track_id", new Random().nextInt());
        event.put("distinct_id", "12345");
        event.put("event", "test");
        Map<Object, Object> properties = new HashMap<>();
        properties.put("test", "test");
        event.put("properties", properties);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        FailedData failedData = new FailedData("", list);
        try {
            SensorsAnalyticsUtil.assertFailedData(failedData);
            Assertions.fail();
        } catch (InvalidArgumentException e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void checkAssertFailedDataWithInvalidPropertiesKey() {
        Map<String, Object> event = new HashMap<>();
        event.put("_track_id", new Random().nextInt());
        event.put("distinct_id", "12345");
        event.put("event", "test");
        Map<Object, Object> properties = new HashMap<>();
        properties.put("distinct_id", "test");
        event.put("properties", properties);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        FailedData failedData = new FailedData("", list);
        try {
            SensorsAnalyticsUtil.assertFailedData(failedData);
            Assertions.fail();
        } catch (InvalidArgumentException e) {
            Assertions.assertTrue(true);
        }
    }

    @Test
    public void checkAssertIDMappingFailedData() {
        Map<String, Object> event = new HashMap<>();
        event.put("_track_id", new Random().nextInt());
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "12345")
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "fz@163.com")
                        .build();
        event.put("identities", identity.getIdentityMap());
        event.put("distinct_id", "12345");
        event.put("type", "track");
        event.put("event", "test");
        Map<Object, Object> properties = new HashMap<>();
        properties.put("test", "test");
        event.put("properties", properties);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        try {
            SensorsAnalyticsUtil.assertFailedData(new FailedData("", list));
        } catch (InvalidArgumentException e) {
            Assertions.fail();
        }
    }

    @Test
    public void checkAssertFailedData() {
        Map<String, Object> event = new HashMap<>();
        event.put("_track_id", new Random().nextInt());
        event.put("distinct_id", "12345");
        event.put("type", "track");
        event.put("event", "test");
        Map<Object, Object> properties = new HashMap<>();
        properties.put("test", "test");
        event.put("properties", properties);
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        try {
            SensorsAnalyticsUtil.assertFailedData(new FailedData("", list));
        } catch (InvalidArgumentException e) {
            Assertions.fail();
        }
    }
}
