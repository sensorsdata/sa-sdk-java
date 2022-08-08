package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;
import com.sensorsdata.analytics.javasdk.consumer.LogSplitMode;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

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

  @Test
  public void checkInit() {
    new ConcurrentLoggingConsumer("file.log");
    new ConcurrentLoggingConsumer("file.log", 30);
    new ConcurrentLoggingConsumer("file.log", "lock.name", 20);
    new ConcurrentLoggingConsumer("file.log", "lock.name", 20, LogSplitMode.DAY);
    assertTrue(true);
  }

  @Test
  public void testInit01() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log");
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test", properties);
  }

  @Test
  public void testInit02() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log", 100);
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test01", properties);
    sa.track("123", true, "test01", properties);
  }

  @Test
  public void testInit03() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log", "lock.log");
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test01", properties);
    sa.track("123", true, "test01", properties);
  }

  @Test
  public void testInit04() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log", "lock.log", 100);
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test01", properties);
    sa.track("123", true, "test01", properties);
  }

  @Test
  public void testInit05() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log", "lock.log", 100);
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test01", properties);
    sa.track("123", true, "test01", properties);
  }

  @Test
  public void testInit06() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log", "lock.log", 100, LogSplitMode.DAY);
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test01", properties);
    sa.track("123", true, "test01", properties);
  }

  @Test
  public void testInit07() throws InvalidArgumentException {
    consumer = new ConcurrentLoggingConsumer("test.log", "lock.log", 100, LogSplitMode.HOUR);
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.track("123", true, "test01", properties);
    sa.track("123", true, "test01", properties);
  }
}
