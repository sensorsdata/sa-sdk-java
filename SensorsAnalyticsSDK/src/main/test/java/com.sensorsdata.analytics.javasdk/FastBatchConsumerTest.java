package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.consumer.Callback;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

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

}
