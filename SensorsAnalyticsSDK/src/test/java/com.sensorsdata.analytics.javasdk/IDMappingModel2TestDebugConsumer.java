package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.consumer.DebugConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Test;

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
public class IDMappingModel2TestDebugConsumer extends SensorsBaseTest {

  private BatchConsumer batchConsumer;

  private List<Map<String, Object>> messageList;
  private ConcurrentLoggingConsumer consumer;

  private StringBuilder messageBuffer;
  SensorsAnalytics sa;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    String url = "http://10.120.111.143:8106/sa?project=default";
    DebugConsumer consumer = new DebugConsumer(url, true);
    sa = new SensorsAnalytics(consumer);
  }

  /**
   * 校验调用 track 方法生成事件节点数是否完整
   */
  @Test
  public void checkTrackEventLoginTrue() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("$time", new Date());
    sa.track("123", true, "test", properties);
  }

  /**
   * 校验调用 track 方法生成事件节点数是否完整
   */
  @Test
  public void checkTrackEventLoginFalse() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    sa.track("123", false, "test", properties);
  }

  /**
   * 校验 trackSignup 记录节点
   */
  @Test
  public void checkTrackSignUp() throws InvalidArgumentException {
    sa.trackSignUp("123", "345");
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
    sa.trackSignUp("123", "345");
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
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void checkProfileSetDataType01() throws InvalidArgumentException {
    sa.profileSet("123", true, "number1", 1234);
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
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("number1", 1234);
    sa.profileIncrement("123", true, properties);

  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement01() throws InvalidArgumentException {
    sa.profileIncrement("123", true, "number1", 1234);
  }


  @Test
  public void testProfileAppend() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", list);

    sa.profileAppend("123", true, properties);

  }

  @Test
  public void testProfileAppend01() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("eee");

    sa.profileAppend("123", true, "list1", "eee");
  }

  // profileUnset
  @Test
  public void testProfileUnset() throws InvalidArgumentException{
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);

    sa.profileUnset("123", true, properties);

    sa.flush();
  }

  // profileUnset
  @Test
  public void testProfileUnset01() throws InvalidArgumentException{
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);

    sa.profileUnset("123", true, "list1");

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
            .build();
    sa.track(eventRecord);
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
            .build();
    sa.profileSet(userRecord);
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
            .build();
    sa.profileSetOnce(userRecord);
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
            .build();
    sa.profileIncrement(userRecord);
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
            .build();
    sa.profileAppend(userRecord);
  }

  // profileUnsetById
  @Test
  public void testProfileUnsetByIdEventBuilder() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .build();
    sa.profileUnset(userRecord);
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
            .build();
    sa.profileDelete(userRecord);
  }
}
