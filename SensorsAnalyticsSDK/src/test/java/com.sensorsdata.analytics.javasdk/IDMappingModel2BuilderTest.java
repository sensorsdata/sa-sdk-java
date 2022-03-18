package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
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
 * 普通模式校验
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/18 23:36
 */
public class IDMappingModel2BuilderTest extends SensorsBaseTest {

  private BatchConsumer batchConsumer;

  private List<Map<String, Object>> messageList;
  private ConcurrentLoggingConsumer consumer;

  private StringBuilder messageBuffer;
  SensorsAnalytics sa;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    batchConsumer = new BatchConsumer("http://10.120.73.51:8106/sa?project=default&token=", 100, true, 3);
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
    assertEquals("abc", messageList.get(0).get("distinct_id"));
    assertNull(messageList.get(0).get("$is_login_id"));
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertNull(messageListult.get("$is_login_id"));
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
    assertEquals("abc", messageList.get(0).get("distinct_id"));
    assertNull(messageList.get(0).get("$is_login_id"));
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertTrue((Boolean) messageListult.get("$is_login_id"));
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
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
    assertEquals(date, messageListult.get("date1"));
    assertEquals("str", messageListult.get("String1"));
    assertFalse((Boolean) messageListult.get("boolean1"));
    assertTrue(messageListult.get("list1") instanceof List<?>);
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
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
    assertEquals(date, messageListult.get("date1"));
    assertEquals("str", messageListult.get("String1"));
    assertFalse((Boolean) messageListult.get("boolean1"));
    assertTrue(messageListult.get("list1") instanceof List<?>);
  }

  /**
   * 校验自定义属性格式是否正常
   */
  @Test
  public void testProfileIncrement() throws InvalidArgumentException {
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
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234, messageListult.get("number1"));
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
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .addProperty("list1", list)
            .build();
    sa.profileAppend(userRecord);
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(list, messageListult.get("list1"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_append", messageList.get(0).get("type"));
  }

  // profileUnsetById
  @Test
  public void testProfileUnsetById() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .addProperty("list1", list)
            .build();
    sa.profileUnset(userRecord);
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(list, messageListult.get("list1"));
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
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    UserRecord userRecord = UserRecord.builder()
            .setDistinctId("123")
            .isLoginId(true)
            .addProperty("list1", list)
            .build();
    sa.profileDelete(userRecord);
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(list, messageListult.get("list1"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_delete", messageList.get(0).get("type"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
  }

  @Test
  public void TestProfileIncrementInvaild01() throws Exception {
    // 2. 用户注册登录之后，系统分配的注册ID
    String registerId = "123456";
    // 2.2 用户注册时，填充了一些个人信息，可以用Profile接口记录下来
    List<String> interests = new ArrayList<String>();
    interests.add("movie");
    interests.add("swim");

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("myName", "name001")
              .build();
      sa.profileIncrement(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("isStudent", true)
              .build();
      sa.profileIncrement(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("$signup_time", Calendar.getInstance().getTime())
              .build();
      sa.profileIncrement(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("Gender", "male")
              .build();
      sa.profileIncrement(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("interest", interests)
              .build();
      sa.profileIncrement(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }
  }

  @Test
  public void TestProfileAppendInvaild() throws Exception {
    // 2. 用户注册登录之后，系统分配的注册ID
    String registerId = "123456";
    // 2.2 用户注册时，填充了一些个人信息，可以用Profile接口记录下来
    List<String> interests = new ArrayList<String>();
    interests.add("movie");
    interests.add("swim");

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("myName", "name001")
              .build();
      sa.profileAppend(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("isStudent", true)
              .build();
      sa.profileAppend(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("$signup_time", Calendar.getInstance().getTime())
              .build();
      sa.profileAppend(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("Gender", "male")
              .build();
      sa.profileAppend(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }

    try {
      UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
              .addProperty("Age", 20)
              .build();
      sa.profileAppend(userRecord);
    }catch (Exception e){
      e.printStackTrace();
    }
    sa.flush();
  }

}
