package com.sensorsdata.analytics.javasdk;


import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import org.junit.Before;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SensorsBaseTest {

  protected final List<Map<String, Object>> res = new ArrayList<>();

  protected SensorsAnalytics sa;

  class TestConsumer implements Consumer {

    @Override
    public void send(Map<String, Object> message) {
      res.add(message);
    }

    @Override
    public void flush() {

    }

    @Override
    public void close() {

    }
  }

  @Before
  public void clearListAndInit() {
    sa = new SensorsAnalytics(new TestConsumer());
    res.clear();
  }

}
