package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * BatchConsumer 单测
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/25 15:09
 */
public class BatchConsumerTest {

  private BatchConsumer batchConsumer;

  private List<Map<String, Object>> messageList;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    batchConsumer = new BatchConsumer("http://localhost:8016/sa", 1, 3, true);
    Field field = batchConsumer.getClass().getDeclaredField("messageList");
    field.setAccessible(true);
    messageList = (List<Map<String, Object>>) field.get(batchConsumer);
  }

  @Test
  public void checkSendData() {
    assertEquals(0, messageList.size());
    Map<String, Object> event = new HashMap<>();
    event.put("distinct_id", "12345");
    event.put("event", "test");
    event.put("type", "track");
    batchConsumer.send(event);
    assertEquals(1, messageList.size());
    messageList.clear();
  }

  @Test
  public void checkFlushDataWithErrorUrl() {
    assertEquals(0, messageList.size());
    Map<String, Object> event = new HashMap<>();
    event.put("distinct_id", "12345");
    event.put("event", "test");
    event.put("type", "track");
    messageList.add(event);
    try {
      batchConsumer.flush();
      fail();
    } catch (Exception e) {
      assertTrue(true);
    }
  }

}
