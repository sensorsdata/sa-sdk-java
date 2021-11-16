package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * 普通模式校验
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/18 23:36
 */
public class NormalModelTest extends SensorsBaseTest {

  /**
   * 校验调用 track 方法生成事件节点数是否完整
   */
  @Test
  public void checkTrackEvent() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test", properties);
    assertEquals(1, res.size());
    assertEquals("123", res.get(0).get("distinct_id"));
    assertEquals("test", res.get(0).get("event"));
    assertNotNull(res.get(0).get("time"));
    assertNotNull(res.get(0).get("_track_id"));
    assertEquals("track", res.get(0).get("type"));
    assertNotNull(res.get(0).get("properties"));
    assertNotNull(res.get(0).get("project"));
    assertNotNull(res.get(0).get("token"));
  }

  /**
   * 校验 event Builder 模式生成数据用户属性是否正常
   */
  @Test
  public void checkTrackEventBuilder() throws InvalidArgumentException {
    EventRecord eventRecord = EventRecord.builder()
        .setDistinctId("abc")
        .isLoginId(false)
        .setEventName("test")
        .build();
    sa.track(eventRecord);
    assertEquals("abc", res.get(0).get("distinct_id"));
    assertNull(res.get(0).get("$is_login_id"));
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
    assertNull(result.get("$is_login_id"));
  }

  /**
   * 校验 is_login_id 为 true 的事件属性
   */
  @Test
  public void checkTrackEventBuilderLoginIdIsTrue() throws InvalidArgumentException {
    EventRecord eventRecord = EventRecord.builder()
        .setDistinctId("abc")
        .isLoginId(true)
        .setEventName("test")
        .build();
    sa.track(eventRecord);
    assertEquals("abc", res.get(0).get("distinct_id"));
    assertNull(res.get(0).get("$is_login_id"));
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
    assertTrue((Boolean) result.get("$is_login_id"));
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void checkProfileSetDataType() throws InvalidArgumentException {
    List<String> list = new ArrayList<>();
    Date date = new Date();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
        .setDistinctId("123")
        .isLoginId(true)
        .addProperty("number1", 1234)
        .addProperty("date1", date)
        .addProperty("String1", "str")
        .addProperty("boolean1", false)
        .addProperty("list1", list)
        .build();
    sa.profileSet(userRecord);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
    assertEquals(1234, result.get("number1"));
    assertEquals(date, result.get("date1"));
    assertEquals("str", result.get("String1"));
    assertFalse((Boolean) result.get("boolean1"));
    assertTrue(result.get("list1") instanceof List<?>);
  }

  /**
   * 校验 trackSignup 记录节点
   */
  @Test
  public void checkTrackSignUp() throws InvalidArgumentException {
    sa.trackSignUp("123", "345");
    assertEquals("track_signup", res.get(0).get("type"));
    assertEquals("123", res.get(0).get("distinct_id"));
    assertEquals("345", res.get(0).get("original_id"));
    assertEquals("$SignUp", res.get(0).get("event"));
  }

}
