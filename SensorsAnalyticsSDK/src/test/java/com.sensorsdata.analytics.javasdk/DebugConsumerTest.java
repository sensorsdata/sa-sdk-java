package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.consumer.DebugConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

import java.util.Calendar;

/**
 * debugConsumer 单元测试
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/01/18 18:55
 */
public class DebugConsumerTest extends SensorsBaseTest {

  @Test
  public void checkDataSend() throws InvalidArgumentException {
    DebugConsumer consumer = new DebugConsumer("http://localhost:8888/sa", true);
    SensorsAnalytics sa = new SensorsAnalytics(consumer);
    EventRecord firstRecord = EventRecord.builder().setDistinctId("a123").isLoginId(Boolean.FALSE)
        .setEventName("track")
        .addProperty("$time", Calendar.getInstance().getTime())
        .addProperty("Channel", "baidu")
        .addProperty("$project", "abc")
        .addProperty("$token", "123")
        .build();
    sa.track(firstRecord);
  }
}
