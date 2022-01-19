package com.sensorsdata.analytics.javasdk;


import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.Before;
import org.junit.BeforeClass;

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

  @BeforeClass
  public static void mockABServer() throws Exception {
    Server server = new Server(8888);
    ServletContextHandler handler = new ServletContextHandler();
    handler.addServlet(new ServletHolder(new TestServlet()), "/debug");
    handler.addServlet(new ServletHolder(new TestServlet()), "/sa");
    server.setHandler(handler);
    server.start();
  }

  @Before
  public void clearListAndInit() {
    sa = new SensorsAnalytics(new TestConsumer());
    res.clear();
  }

}
