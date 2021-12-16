package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.Callback;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

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
        .addIdentityProperty("email", "test@mail.com")
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
        .addIdentityProperty("email", "test@mail.com")
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


  private FastBatchConsumer rightConsumer =
      new FastBatchConsumer("http://10.129.138.189:8106/sa?project=default", new Callback() {
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
        .addIdentityProperty("email", "test@mail.com")
        .build();
    sa.trackById(identity, "test", null);
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
   * 重发送 idMapping profile 事件失败
   */
  @Test
  public void checkIdMappingProfileRight() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("email", "test@mail.com")
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

}
