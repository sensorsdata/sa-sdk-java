package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 普通模式校验
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/18 23:36
 */
public class IDMappingModel2Test extends SensorsBaseTest {

  private BatchConsumer batchConsumer;

  private List<Map<String, Object>> messageList;
  private ConcurrentLoggingConsumer consumer;

  private StringBuilder messageBuffer;
  SensorsAnalytics sa;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    batchConsumer = new BatchConsumer("http://10.120.73.51:8106/sa?project=default&token=", 100, 3, true);
    Field field = batchConsumer.getClass().getDeclaredField("messageList");
    field.setAccessible(true);
    messageList = (List<Map<String, Object>>) field.get(batchConsumer);

    consumer = new ConcurrentLoggingConsumer("file.log");
    field = consumer.getClass().getSuperclass().getDeclaredField("messageBuffer");
    field.setAccessible(true);
    messageBuffer = (StringBuilder) field.get(consumer);

//    sa = new SensorsAnalytics(consumer);
    sa = new SensorsAnalytics(batchConsumer);
  }

  /**
   * 校验调用 track 方法生成事件节点数是否完整
   */
  @Test
  public void checkTrackEventLoginTrue() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test", properties);

    assertEquals(1, messageList.size());
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertEquals("test", messageList.get(0).get("event"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("track", messageList.get(0).get("type"));

    assertNotNull(messageList.get(0).get("properties"));
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(true, messageListult.get("$is_login_id"));
    assertNotNull(messageList.get(0).get("project"));
    assertNotNull(messageList.get(0).get("token"));
  }

  /**
   * 校验调用 track 方法生成事件节点数是否完整
   */
  @Test
  public void checkTrackEventLoginFalse() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", false, "test", properties);

    assertEquals(1, messageList.size());
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertEquals("test", messageList.get(0).get("event"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("track", messageList.get(0).get("type"));

    assertNotNull(messageList.get(0).get("properties"));
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertNull(messageListult.get("$is_login_id"));
    assertNotNull(messageList.get(0).get("project"));
    assertNotNull(messageList.get(0).get("token"));
  }

  /**
   * 校验 trackSignup 记录节点
   */
  @Test
  public void checkTrackSignUp() throws InvalidArgumentException {
    sa.trackSignUp("123", "345");
    assertEquals("track_signup", messageList.get(0).get("type"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertEquals("345", messageList.get(0).get("original_id"));
    assertEquals("$SignUp", messageList.get(0).get("event"));
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
    Map<String, Object> properties = new HashMap<>();
    properties.put("number1", 1234);
    properties.put("date1", date);
    properties.put("String1", "str");
    properties.put("boolean1", false);
    properties.put("list1", list);

    sa.profileSet("123", true, properties);

    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
    assertEquals(date, messageListult.get("date1"));
    assertEquals("str", messageListult.get("String1"));
    assertFalse((Boolean) messageListult.get("boolean1"));
    assertTrue(messageListult.get("list1") instanceof List<?>);

    assertTrue((Boolean) messageListult.get("$is_login_id"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_set", messageList.get(0).get("type"));
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void checkProfileSetDataType01() throws InvalidArgumentException {

    sa.profileSet("123", true, "number1", 1234);
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));

    assertTrue((Boolean) messageListult.get("$is_login_id"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_set", messageList.get(0).get("type"));
  }
  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileSetOnceDataType() throws InvalidArgumentException {
    List<String> list = new ArrayList<>();
    Date date = new Date();
    list.add("aaa");
    list.add("bbb");
    Map<String, Object> properties = new HashMap<>();
    properties.put("number1", 1234);
    properties.put("date1", date);
    properties.put("String1", "str");
    properties.put("boolean1", false);
    properties.put("list1", list);
    sa.profileSetOnce("123", true, properties);
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
    assertEquals(date, messageListult.get("date1"));
    assertEquals("str", messageListult.get("String1"));
    assertFalse((Boolean) messageListult.get("boolean1"));
    assertTrue(messageListult.get("list1") instanceof List<?>);

    // 其他字段检查
    assertTrue((Boolean) messageListult.get("$is_login_id"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_set_once", messageList.get(0).get("type"));
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("number1", 1234);
    sa.profileIncrement("123", true, properties);
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
    // 其他字段检查
    assertTrue((Boolean) messageListult.get("$is_login_id"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_increment", messageList.get(0).get("type"));
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement01() throws InvalidArgumentException {
    sa.profileIncrement("123", true, "number1", 1234);
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
    // 其他字段检查
    assertTrue((Boolean) messageListult.get("$is_login_id"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_increment", messageList.get(0).get("type"));
  }


  @Test
  public void testProfileAppendById() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", list);

    sa.profileAppend("123", true, properties);
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(list, messageListult.get("list1"));
    // 其他字段检查
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_append", messageList.get(0).get("type"));
  }

  @Test
  public void testProfileAppendById01() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("eee");

    sa.profileAppend("123", true, "list1", "eee");
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(list, messageListult.get("list1"));
    // 其他字段检查
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_append", messageList.get(0).get("type"));
  }

  // profileUnsetById
  @Test
  public void testProfileUnsetById() throws InvalidArgumentException{
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);

    sa.profileUnset("123", true, properties);
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertTrue((Boolean) messageListult.get("list1"));
    // 其他字段检查
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_unset", messageList.get(0).get("type"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    sa.flush();
  }

  // profileUnsetById
  @Test
  public void testProfileUnsetById01() throws InvalidArgumentException{
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);

    sa.profileUnset("123", true, "list1");
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertTrue((Boolean) messageListult.get("list1"));
    // 其他字段检查
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_unset", messageList.get(0).get("type"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    sa.flush();
  }

  // profileDeleteById
  @Test
  public void testProfileDeleteById() throws InvalidArgumentException{
    sa.profileDelete("123", true);

    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_delete", messageList.get(0).get("type"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
  }

  @Test
  public void TestItemSet_Delete() throws Exception {
    //物品纬度表上报
    String itemId = "product001", itemType = "mobile";
    ItemRecord addRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemType)
            .addProperty("color", "white")
            .build();
    sa.itemSet(addRecord);

    //删除物品纬度信息
    ItemRecord deleteRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemType)
            .build();
    sa.itemDelete(deleteRecord);
    sa.flush();
  }
}
