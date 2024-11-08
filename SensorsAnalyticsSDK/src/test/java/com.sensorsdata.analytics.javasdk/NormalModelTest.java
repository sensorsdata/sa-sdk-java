package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * 普通模式校验
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/18 23:36
 */
public class NormalModelTest extends SensorsBaseTest {

    /** 校验调用 track 方法生成事件节点数是否完整 */
    @Test
    public void checkTrackEvent() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("test", "test");
        properties.put("$project", "abc");
        properties.put("$token", "123");
        sa.track("123", true, "test", properties);
        assertEventData(data);
    }

    /** 校验 event Builder 模式生成数据用户属性是否正常 */
    @Test
    public void checkTrackEventBuilder() throws InvalidArgumentException {
        EventRecord eventRecord =
                EventRecord.builder()
                        .setDistinctId("abc")
                        .isLoginId(false)
                        .setEventName("test")
                        .build();
        sa.track(eventRecord);
        Assertions.assertEquals("abc", data.get("distinct_id"));
        Assertions.assertNull(data.get("$is_login_id"));
        Map<?, ?> result = (Map<?, ?>) data.get("properties");
        Assertions.assertNull(result.get("$is_login_id"));
    }

    /** 校验 is_login_id 为 true 的事件属性 */
    @Test
    public void checkTrackEventBuilderLoginIdIsTrue() throws InvalidArgumentException {
        EventRecord eventRecord =
                EventRecord.builder()
                        .setDistinctId("abc")
                        .isLoginId(true)
                        .setEventName("test")
                        .build();
        sa.track(eventRecord);
        Assertions.assertEquals("abc", data.get("distinct_id"));
        Assertions.assertNull(data.get("$is_login_id"));
        Map<?, ?> result = (Map<?, ?>) data.get("properties");
        Assertions.assertTrue((Boolean) result.get("$is_login_id"));
    }

    /** 校验自定义属性格式是否正常 */
    @Test
    public void checkProfileSetDataType() throws InvalidArgumentException {
        List<String> list = new ArrayList<>();
        Date date = new Date();
        list.add("aaa");
        list.add("bbb");
        UserRecord userRecord =
                UserRecord.builder()
                        .setDistinctId("123")
                        .isLoginId(true)
                        .addProperty("number1", 1234)
                        .addProperty("date1", date)
                        .addProperty("String1", "str")
                        .addProperty("boolean1", false)
                        .addProperty("list1", list)
                        .build();
        sa.profileSet(userRecord);
        Map<?, ?> result = (Map<?, ?>) data.get("properties");
        Assertions.assertEquals(1234, result.get("number1"));
        Assertions.assertEquals(date, result.get("date1"));
        Assertions.assertEquals("str", result.get("String1"));
        Assertions.assertFalse((Boolean) result.get("boolean1"));
        Assertions.assertTrue(result.get("list1") instanceof List<?>);
    }

    /** 校验 trackSignup 记录节点 */
    @Test
    public void checkTrackSignUp() throws InvalidArgumentException {
        sa.trackSignUp("123", "345");
        assertEventData(data);
    }

    @Test
    public void checkCommonProperties() throws InvalidArgumentException {
        Map<String, Object> map = new HashMap<>();
        map.put("key1", "value1");
        sa.registerSuperProperties(map);
        Map<String, Object> properties = new HashMap<>();
        properties.put("key1", "value2");
        sa.track("123", true, "test1", properties);
        assertEventData(data);
        Assertions.assertEquals("value2", ((Map) data.get("properties")).get("key1").toString());
    }
}
