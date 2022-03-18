package com.sensorsdata.analytics.javasdk;


import com.sensorsdata.analytics.javasdk.consumer.Consumer;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.*;
import static org.junit.Assert.*;

public class SensorsBaseTest {
  /**
   * 单元测试中若生成多条数据，则存放到该集合中
   */
  protected final List<Map<String, Object>> res = new ArrayList<>();
  /**
   * SDK 最后一次生成的数据集合
   */
  protected Map<String, Object> data = null;
  /**
   * 初始化 sa
   */
  protected SensorsAnalytics sa = null;
  /**
   * mock server
   */
  protected static Server server = null;

  protected String url = "http://localhost:8888/sa";


  class TestConsumer implements Consumer {

    @Override
    public void send(Map<String, Object> message) {
      data = message;
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
    server = new Server(8888);
    ServletContextHandler handler = new ServletContextHandler();
    handler.addServlet(new ServletHolder(new TestServlet()), "/debug");
    handler.addServlet(new ServletHolder(new TestServlet()), "/sa");
    server.setHandler(handler);
    server.start();
  }

  @Test
  public void checkServer() {
    assertNotNull(server);
  }

  @AfterClass
  public static void closeMockServer() throws Exception {
    if (server != null) {
      server.stop();
    }
  }

  @Before
  public void clearListAndInit() {
    sa = new SensorsAnalytics(new TestConsumer());
    data = null;
    res.clear();
  }

  /**
   * 校验非 IDM 模式下 event 数据节点
   * properties 节点可能没有数据，所以需放到具体的单元测试里面去判断
   *
   * @param data 数据节点
   */
  protected void assertEventData(Map<String, Object> data) {
    assertUserData(data);
    assertTrue("数据中没有 event 节点！", data.containsKey("event"));
  }

  /**
   * 校验非 IDM 模式下 user 数据节点
   *
   * @param data 数据节点
   */
  protected void assertUserData(Map<String, Object> data) {
    assertNotNull(data);
    assertTrue("数据中没有 _track_id 节点！", data.containsKey("_track_id"));
    assertTrue("数据中没有 lib 节点！", data.containsKey("lib"));
    assertTrue("数据中没有 distinct_id 节点！", data.containsKey("distinct_id"));
    assertTrue("数据中没有 time 节点！", data.containsKey("time"));
    assertTrue("数据中没有 type 节点！", data.containsKey("type"));
    assertFalse("数据中包含 item_id 节点！", data.containsKey("item_id"));
    assertFalse("数据中包含 item_type 节点！", data.containsKey("item_type"));
  }

  /**
   * 校验非 IDM 模式下 item 数据节点
   *
   * @param data 数据节点
   */
  protected void assertItemData(Map<String, Object> data) {
    assertNotNull(data);
    assertTrue("item 数据中没有 lib 节点！", data.containsKey("lib"));
    assertTrue("item 数据中没有 item_id 节点！", data.containsKey("item_id"));
    assertTrue("item 数据中没有 item_type 节点！", data.containsKey("item_type"));
    assertTrue("item 数据中没有 time 节点！", data.containsKey("time"));
    assertTrue("item 数据中没有 type 节点！", data.containsKey("type"));
    assertFalse("item 数据中包含 distinct_id 节点！", data.containsKey("distinct_id"));
    assertFalse("item 数据中包含 _track_id 节点！", data.containsKey("_track_id"));
  }

  /**
   * 校验 IDM3.0 模式下 event 数据节点
   *
   * @param data 数据节点
   */
  protected void assertIDM3EventData(Map<String, Object> data) {
    assertEventData(data);
    assertTrue("IDM3 数据中没有 identities 节点！", data.containsKey("identities"));
  }

  /**
   * 校验 IDM3.0 模式下 user 数据节点
   *
   * @param data 数据节点
   */
  protected void assertIDM3UserData(Map<String, Object> data) {
    assertUserData(data);
    assertTrue("IDM3 数据中没有 identities 节点！", data.containsKey("identities"));
  }

  /**
   * 校验 lib 内容
   *
   * @param lib lib 信息集合
   */
  protected void assertDataLib(Object lib) {
    assertTrue(lib instanceof Map);
    Map<String, String> libMap = (Map<String, String>) lib;
    assertFalse("数据中 lib 节点信息不存在！", libMap.isEmpty());
    assertTrue("lib 节点信息不存在 $lib 信息！", libMap.containsKey(LIB_SYSTEM_ATTR));
    assertTrue("lib 节点信息不存在 $lib_version 信息！", libMap.containsKey(LIB_VERSION_SYSTEM_ATTR));
    assertTrue("lib 节点信息不存在 $lib_method 信息！", libMap.containsKey(LIB_METHOD_SYSTEM_ATTR));
  }


}
