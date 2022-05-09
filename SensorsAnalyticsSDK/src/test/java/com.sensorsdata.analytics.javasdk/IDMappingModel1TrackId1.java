package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;

import static org.junit.Assert.*;

/**
 *  适用于 v3.4.4+ 版本
 *  测试点：验证事件的 _track_id 正常由 $track_id 生成。
 *  无特殊情况，不在下面的 testcase 上一一说明
 */
public class IDMappingModel1TrackId1 extends SensorsBaseTest {

  private BatchConsumer batchConsumer;

  private List<Map<String, Object>> messageList;
  private ConcurrentLoggingConsumer consumer;

  private StringBuilder messageBuffer;
  SensorsAnalytics sa;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    String url = "http://10.120.111.143:8106/sa?project=default";
    // 注意要设置 bulkSize 稍微大一点，这里设置为 100，否则超过 1 条就上报，messageList 里面拿不到事件数据
    batchConsumer = new BatchConsumer(url, 100, true, 3);
    // 通过反射机制获取 BatchConsumer 的 messageList
    Field field = batchConsumer.getClass().getDeclaredField("messageList"); // messageList 是 BatchConsumer 用来暂存事件数据的成员变量
    field.setAccessible(true);
    messageList = (List<Map<String, Object>>) field.get(batchConsumer);
    sa = new SensorsAnalytics(batchConsumer);
  }

  private void assertNotNullProp(){
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertNotNull(messageList.get(0).get("properties"));
  }

  /**
   * 校验调用 track 方法生成事件节点数是否完整
   */
  @Test
  public void checkTrackEventLoginTrue() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("$track_id", 111);

    sa.track("123", true, "test", properties);
    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id

  }


  /**
   * 校验 trackSignup 记录节点
   */
  @Test
  public void checkTrackSignUpProp() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("number1", 1234);
    properties.put("String1", "str");
    properties.put("boolean1", false);
    properties.put("$track_id", 111);
    sa.trackSignUp("123", "345",  properties);

    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
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
    properties.put("$track_id", 111);
    sa.profileSet("123", true, properties);

    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    assertTrue(props.containsKey("number1")); // properties 包含其他自定义属性

  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void checkProfileSetDataType01() throws InvalidArgumentException {

    sa.profileSet("123", true, "$track_id", 111);
    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id

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
    properties.put("$track_id", 111);
    sa.profileSetOnce("123", true, properties);

    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    assertTrue(props.containsKey("number1")); // properties 包含其他自定义属性

  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("number1", 1234);
    properties.put("$track_id", 111);
    sa.profileIncrement("123", true, properties);

    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
    assertTrue(props.containsKey("number1")); // properties 包含其他自定义属性


  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement01() throws InvalidArgumentException {
    sa.profileIncrement("123", true, "$track_id", 111);

    assertNotNullProp();

    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }


  @Test
  public void testProfileAppend() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", list);

    List<Integer> listInt = new ArrayList<>();
    listInt.add(111);
    properties.put("$track_id", listInt);

    try {
      sa.profileAppend("123", true, properties);
      fail("[ERROR] profileAppend should throw InvalidArgumentException.");
    }catch (Exception e){
      e.printStackTrace();
      assertEquals("The property '$track_id' should be a list of String.", e.getMessage());
    }

  }

  @Test
  public void testProfileAppend01() throws InvalidArgumentException{
    sa.profileAppend("123", true, "$track_id", "111");

    assertNotNullProp();

    assertNotEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id

  }

  // profileUnset
  @Test
  public void testProfileUnset() {
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);
    properties.put("$track_id", 111);

    try {
      sa.profileUnset("123", true, properties);
      fail("[ERROR] profileUnset should throw InvalidArgumentException.");
    }catch (InvalidArgumentException e){
      assertEquals("The property value of $track_id should be true.", e.getMessage());
    }

  }

  // profileUnset
  @Test
  public void testProfileUnset01() throws InvalidArgumentException{
    sa.profileUnset("123", true, "$track_id");

    assertNotNullProp();

    assertNotEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }

  // profileDelete
  @Test
  public void testProfileDelete() throws InvalidArgumentException{
    sa.profileDelete("123", true);
  }

  @Test
  public void testItemSet_Delete() throws Exception {
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



  /**
   * 校验 event Builder 模式生成数据用户属性是否正常
   */
  @Test
  public void checkTrackEventBuilder() throws InvalidArgumentException {
    EventRecord eventRecord = EventRecord.builder()
            .setDistinctId("abc")
            .isLoginId(false)
            .setEventName("test")
            .addProperty("$track_id", 111)
            .build();
    sa.track(eventRecord);

    assertNotNullProp();
    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
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
            .addProperty("$track_id", 111)
            .build();
    sa.track(eventRecord);

    assertNotNullProp();
    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void checkProfileSetDataTypeEventBuilder() throws InvalidArgumentException {
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
            .addProperty("$track_id", 111)
            .build();
    sa.profileSet(userRecord);

    assertNotNullProp();
    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileSetOnceDataTypeEventBuilder() throws InvalidArgumentException {
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
            .addProperty("$track_id", 111)
            .build();
    sa.profileSetOnce(userRecord);

    assertNotNullProp();
    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrementEventBuilder() throws InvalidArgumentException {
    List<String> list = new ArrayList<>();
    Date date = new Date();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .addProperty("number1", 1234)
            .addProperty("$track_id", 111)
            .build();
    sa.profileIncrement(userRecord);

    assertNotNullProp();
    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }

  @Test
  public void testProfileAppendByIdEventBuilder() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .addProperty("list1", list)
            .addProperty("$track_id", 111)
            .build();
    try {
      sa.profileAppend(userRecord);
      fail("profileAppend should throw InvalidArgumentException");
    }catch (InvalidArgumentException e){
      assertEquals("The property value of PROFILE_APPEND should be a List<String>.", e.getMessage());
    }

  }

  // profileUnsetById
  @Test
  public void testProfileUnsetByIdEventBuilder() {
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = null;
    try {
      userRecord = UserRecord.builder()
              .setDistinctId("123")
              .isLoginId(true)
              .addProperty("$track_id", 111)
              .build();
      sa.profileUnset(userRecord);
      fail("[ERROR] profileUnset should throw InvalidArgumentException.");
    }catch (InvalidArgumentException e){
      assertEquals("The property value of $track_id should be true.", e.getMessage());
    }

  }

  // profileDeleteById
  @Test
  public void testProfileDeleteByIdEventBuilder() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .addProperty("list1", list)
            .addProperty("$track_id", 111)
            .build();
    sa.profileDelete(userRecord);

    assertNotNullProp();
    assertEquals(111, messageList.get(0).get("_track_id"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertFalse(props.containsKey("$track_id")); // properties 不包含 $track_id
  }
}
