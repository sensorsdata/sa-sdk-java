package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.consumer.Callback;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * FastBatchConsumer 单测
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/24 19:08
 */
public class FastBatchConsumerTest {

  private FastBatchConsumer consumer;

  private LinkedBlockingQueue<Map<String, Object>> buffer;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    consumer = new FastBatchConsumer("http://localhost:8080/test", new Callback() {
      @Override
      public void onFailed(FailedData failedData) {
        System.out.println(failedData);
      }
    });
    Field field = consumer.getClass().getDeclaredField("buffer");
    field.setAccessible(true);
    buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);
  }

  /**
   * 调用 send 接口，数据是否保存到缓存中
   */
  @Test
  public void checkSendData() {
    assertEquals(0, buffer.size());
    Map<String, Object> event = new HashMap<>();
    event.put("distinct_id", "12345");
    event.put("event", "test");
    event.put("type", "track");
    consumer.send(event);
    assertEquals(1, buffer.size());
    Map<String, Object> poll = buffer.poll();
    assertNotNull(poll);
    assertEquals(3, poll.size());
  }

  /**
   * 调用 flush 接口，设置错误的 URL，检查数据是否通过回调函数返回
   */
  @Test
  public void checkFlushWithErrorUrl() {
    assertEquals(0, buffer.size());
    Map<String, Object> event = new HashMap<>();
    event.put("distinct_id", "12345");
    event.put("event", "test");
    event.put("type", "track");
    consumer.send(event);
    consumer.flush();
    assertEquals(0, buffer.size());
  }

  /**
   * 重发送接口，设置错误的 URL，返回发送失败标志
   */
  @Test
  public void checkResendFailedDataWithErrorUrl() throws InvalidArgumentException, JsonProcessingException {
    Map<String, Object> event = new HashMap<>();
    event.put("distinct_id", "12345");
    event.put("event", "test");
    event.put("type", "track");
    ArrayList<Map<String, Object>> list = new ArrayList<>();
    list.add(event);
    FailedData failedData = new FailedData("", list);
    boolean sendFlag = consumer.resendFailedData(failedData);
    assertFalse(sendFlag);
  }

  @Test
  public void checkResendFailedDataWithRightUrl() throws InvalidArgumentException, JsonProcessingException {
    final FastBatchConsumer fastBatchConsumer =
        new FastBatchConsumer("http://10.129.138.189:8106/sa?project=production", new Callback() {
          @Override
          public void onFailed(FailedData failedData) {
            System.out.println(failedData);
          }
        });
    Map<String, Object> event = new HashMap<>();
    event.put("_track_id", new Random().nextInt());
    event.put("distinct_id", "123456");
    event.put("event", "test");
    event.put("type", "track");
    ArrayList<Map<String, Object>> list = new ArrayList<>();
    list.add(event);
    boolean sendFlag = fastBatchConsumer.resendFailedData(new FailedData("", list));
    assertTrue(sendFlag);
  }



}
