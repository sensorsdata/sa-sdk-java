package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;

import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

/**
 * ConcurrentLoggingConsumer 单测
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/25 16:46
 */
public class ConcurrentLoggingConsumerTest {

  private ConcurrentLoggingConsumer consumer;

  private StringBuilder messageBuffer;

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
    consumer = new ConcurrentLoggingConsumer("file.log");
    Field field = consumer.getClass().getSuperclass().getDeclaredField("messageBuffer");
    field.setAccessible(true);
    messageBuffer = (StringBuilder) field.get(consumer);
  }

  @Test
  public void checkSendData() {
    assertEquals(0, messageBuffer.length());
    Map<String, Object> event = new HashMap<>();
    event.put("distinct_id", "12345");
    event.put("event", "test");
    event.put("type", "track");
    consumer.send(event);
    assertNotNull(messageBuffer);
    messageBuffer.setLength(0);
  }
}
