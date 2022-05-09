package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.Callback;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * FastBatchConsumer 重发送接口单测
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/12/16 14:59
 */
public class FastBatchConsumerResendTest {

  private List<FailedData> dataList = new ArrayList<>();

  private ISensorsAnalytics sa;

  private FastBatchConsumer consumer;

  @Before
  public void init() {
    consumer = new FastBatchConsumer("http://localhost:8080/test", new Callback() {
      @Override
      public void onFailed(FailedData failedData) {
        dataList.add(failedData);
      }
    });
    sa = new SensorsAnalytics(consumer);
  }

  /**
   * 重发送 trackSignup 事件失败
   */
  @Test
  public void checkResendTrackSignupError() throws InvalidArgumentException {
    sa.trackSignUp("a1234", "b4567");
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = consumer.resendFailedData(dataList.get(0));
      assertFalse(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 重发送 track 事件失败
   */
  @Test
  public void checkResendTrackError() throws InvalidArgumentException {
    EventRecord eventRecord = EventRecord.builder()
        .setDistinctId("a1234")
        .isLoginId(true)
        .setEventName("test")
        .addProperty("haha", "ddd")
        .build();
    sa.track(eventRecord);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = consumer.resendFailedData(dataList.get(0));
      assertFalse(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 重发送 item 事件失败
   */
  @Test
  public void checkResendItemError() throws InvalidArgumentException {
    ItemRecord itemRecord = ItemRecord.builder()
        .setItemId("test1")
        .setItemType("haha")
        .addProperty("test", "fgh")
        .build();
    sa.itemSet(itemRecord);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = consumer.resendFailedData(dataList.get(0));
      assertFalse(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 重发送 idMapping track 事件失败
   */
  @Test
  public void checkIdMappingTrackError() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "test@mail.com")
        .build();
    sa.trackById(identity, "test", null);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = consumer.resendFailedData(dataList.get(0));
      assertFalse(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 重发送 idMapping profile 事件失败
   */
  @Test
  public void checkIdMappingProfileError() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "test@mail.com")
        .build();
    sa.profileSetById(identity, "test", "ddd");
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = consumer.resendFailedData(dataList.get(0));
      assertFalse(flag);
    } catch (Exception e) {
      fail();
    }
  }


//  String url = "http://newsdktest.datasink.sensorsdata.cn/sa?project=xuchang&token=5a394d2405c147ca";
//  String url = "http://10.120.73.51:8106/sa?project=default&token=";
  String url = "http://10.120.170.245:8106/sa?project=default";
  private FastBatchConsumer rightConsumer =
      new FastBatchConsumer(url, new Callback() {
        @Override
        public void onFailed(FailedData failedData) {
        }
      });

  /**
   * 重发送 trackSignup 事件成功
   */
  @Test
  public void checkResendTrackSignupRight() throws InvalidArgumentException {
    sa.trackSignUp("a1234", "b4567");
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 重发送 track 事件成功
   */
  @Test
  public void checkResendTrackRight() throws InvalidArgumentException {
    EventRecord eventRecord = EventRecord.builder()
        .setDistinctId("a1234")
        .isLoginId(true)
        .setEventName("test")
        .addProperty("haha", "ddd")
        .build();
    sa.track(eventRecord);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 校验自定义属性
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
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
  }


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
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
    // 属性检查
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals(1234L, messageListult.get("number1"));
    // 其他字段检查
    assertTrue((Boolean) messageListult.get("$is_login_id"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_increment", messageList.get(0).get("type"));
  }


  @Test
  public void testProfileAppend() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("aaa");
    list.add("bbb");
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", list);

    sa.profileAppend("123", true, properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
  public void testProfileAppend01() throws InvalidArgumentException{
    List<String> list = new ArrayList<>();
    list.add("eee");

    sa.profileAppend("123", true, "list1", "eee");
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
  public void testProfileUnset() throws InvalidArgumentException{
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);

    sa.profileUnset("123", true, properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
  public void testProfileUnset01() throws InvalidArgumentException{
    Map<String, Object> properties = new HashMap<>();
    properties.put("list1", true);

    sa.profileUnset("123", true, "list1");
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
  public void testProfileDelete() throws InvalidArgumentException{
    sa.profileDelete("123", true);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
    Map<?, ?> messageListult = (Map<?, ?>) messageList.get(0).get("properties");
    assertEquals("123", messageList.get(0).get("distinct_id"));
    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_delete", messageList.get(0).get("type"));
    assertEquals("123", messageList.get(0).get("distinct_id"));
  }

  /**
   * 重发送 item 事件成功
   */
  @Test
  public void checkResendItemRight() throws InvalidArgumentException {
    ItemRecord itemRecord = ItemRecord.builder()
        .setItemId("test1")
        .setItemType("haha")
        .addProperty("test", "fgh")
        .build();
    sa.itemSet(itemRecord);
    sa.itemDelete(itemRecord);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
  }

  /**
   * 重发送 idMapping track 事件失败
   */
  @Test
  public void checkIdMappingTrackRight() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "fz@163.com")
        .build();
    sa.trackById(identity, "test333", null);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
  }


  /**
   * 校验 ID-Mapping bind 接口
   */
  @Test
  public void TestIdMappingBind() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty("$identity_mobile", "123")
            .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "fz@163.com")
            .build();
    sa.bind(identity);

    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();

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
   * 校验 ID-Mapping unbind 接口用户格式
   */
  @Test
  public void TestUnbindUserId() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "123")
            .build();
    sa.unbind(identity);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
    assertNotNull(messageList.get(0).get("identities"));
    assertTrue(messageList.get(0).get("identities") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) messageList.get(0).get("identities");
    assertEquals("123", result.get("$identity_mobile"));
    assertEquals("track_id_unbind", messageList.get(0).get("type"));
  }

  /**
   * 重发送 idMapping profile 事件失败
   */
  @Test
  public void checkIdMappingProfileRight() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "test@mail.com")
        .build();
    sa.profileSetById(identity, "test", "ddd");
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }
  }

  @Test
  public void TestProfileSetById() throws InvalidArgumentException{
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
            .build();
    Map<String, Object> properties = new HashMap<>();
    properties.put("age", 1);
    sa.profileSetById(identity,properties);
//        System.out.println(messageList.isEmpty());

    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
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
    sa.flush();
  }

  @Test
  public void TestProfileSetOnceById() throws InvalidArgumentException{
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
            .build();
    Map<String, Object> properties = new HashMap<>();
    properties.put("age", 1);
    sa.profileSetOnceById(identity,properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();    assertEquals(1, messageList.size());
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
    sa.flush();
  }

  @Test
  public void TestProfileIncrementById() throws InvalidArgumentException{
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
            .build();
    Map<String, Object> properties = new HashMap<>();
    properties.put("age", 1);
    sa.profileIncrementById(identity,properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();    assertEquals(1, messageList.size());
    assertNotNull(messageList.get(0).get("identities"));
    Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
    assertEquals("123", identities.get("$identity_email"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertEquals(1, props.get("age"));
    assertFalse(props.containsKey("$is_login_id"));
    assertFalse(props.containsKey("$is_login_id"));

    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_increment", messageList.get(0).get("type"));
    assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
    sa.flush();
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
    sa.profileAppendById(identity,properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();    assertEquals(1, messageList.size());
    assertNotNull(messageList.get(0).get("identities"));
    Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
    assertEquals("123", identities.get("$identity_email"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertEquals(list, props.get("favorite"));
    assertFalse(props.containsKey("$is_login_id"));

    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_append", messageList.get(0).get("type"));
    assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
    sa.flush();
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
    sa.profileUnsetById(identity,properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();
    assertEquals(1, messageList.size());
    assertNotNull(messageList.get(0).get("identities"));
    Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
    assertEquals("123", identities.get("$identity_email"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertEquals(true, props.get("favorite"));
    assertFalse(props.containsKey("$is_login_id"));

    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_unset", messageList.get(0).get("type"));
    assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
    sa.flush();
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
    sa.profileAppendById(identity,properties);
    sa.flush();
    assertEquals(1, dataList.size());
    assertNotNull(dataList.get(0).getFailedData());
    try {
      boolean flag = rightConsumer.resendFailedData(dataList.get(0));
      assertTrue(flag);
    } catch (Exception e) {
      fail();
    }

    List<Map<String, Object>> messageList = dataList.get(0).getFailedData();    assertEquals(1, messageList.size());
    assertNotNull(messageList.get(0).get("identities"));
    Map<String, Object> identities = (Map<String, Object>) messageList.get(0).get("identities");
    assertEquals("123", identities.get("$identity_email"));

    Map<String, Object> props = (Map<String, Object>)messageList.get(0).get("properties");
    assertEquals(list, props.get("favorite"));
    assertFalse(props.containsKey("$is_login_id"));

    assertNotNull(messageList.get(0).get("time"));
    assertNotNull(messageList.get(0).get("_track_id"));
    assertEquals("profile_append", messageList.get(0).get("type"));
    assertEquals("$identity_email+123", messageList.get(0).get("distinct_id"));
    sa.flush();
  }

}
