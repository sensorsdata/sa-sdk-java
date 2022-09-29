package com.sensorsdata.analytics.javasdk;


import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_DETAIL_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_METHOD_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROPERTIES;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TIME_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ID;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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

  protected static Set<String> actionTypeSet = new HashSet<>();


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
    actionTypeSet.add(SensorsConst.TRACK_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.TRACK_SIGN_UP_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.PROFILE_SET_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.PROFILE_INCREMENT_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.PROFILE_UNSET_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.PROFILE_SET_ONCE_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.PROFILE_APPEND_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.BIND_ID_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.UNBIND_ID_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.PROFILE_DELETE_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.ITEM_SET_ACTION_TYPE);
    actionTypeSet.add(SensorsConst.ITEM_DELETE_ACTION_TYPE);
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

    //增加各字段类型校验
    assertTrue("数据中 time 节点类型不正确！", data.get("time") instanceof Long);
    assertTrue("数据中 _track_id 节点类型不正确！", data.get("_track_id") instanceof Integer);
    assertTrue("数据中 distinct_id 节点类型不正确！", data.get("distinct_id") instanceof String);
    assertTrue("数据中 type 节点类型不正确！", data.get("type") instanceof String);
    assertTrue("数据中 lib 节点类型不正确！", data.get("lib") instanceof Map);


    assertFalse("数据中包含 item_id 节点！", data.containsKey("item_id"));
    assertFalse("数据中包含 item_type 节点！", data.containsKey("item_type"));
    if (data.containsKey(PROPERTIES)) {
      assertProperties(data.get(PROPERTIES));
    }
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
    assertTrue("lib 节点信息不存在 $lib_detail 信息！", libMap.containsKey(LIB_DETAIL_SYSTEM_ATTR));

  }

  /**
   * 校验 properties
   * 校验原则：如果 properties 存在，判断 $time 和 $track_id 是否存在
   */
  protected void assertProperties(Object properties) {
    assertTrue(properties instanceof Map);
    Map<String, Object> propertiesMap = (Map<String, Object>) properties;
    assertFalse("properties 中的 $time 属性并未删除", propertiesMap.containsKey(TIME_SYSTEM_ATTR));
    assertFalse("properties 中的 $track_id 属性并未删除", propertiesMap.containsKey(TRACK_ID));
  }

  /**
   * 校验 userEventSchema 数据
   */
  protected void assertUESData(Map<String, Object> data) {
    assertIESData(data);
    Object properties = data.get("properties");
    assertTrue("数据节点 properties 类型不正确！", properties instanceof Map);
    Map<String, String> proMap = (Map<String, String>) properties;
    assertTrue("数据中用户信息节点（identities+distinct_id/user_id）丢失！",
        (proMap.containsKey("identities") && proMap.containsKey("distinct_id")) || proMap.containsKey("user_id"));
  }

  protected void assertEventIdentitiesInfo(Map<String, Object> data, String expectDistinctId) {
    Map<String, String> proMap = (Map<String, String>) data.get("properties");
    assertTrue(proMap.containsKey("identities"));
    assertTrue(proMap.containsKey("distinct_id"));
    assertEquals(expectDistinctId, proMap.get("distinct_id").toString());
  }

  /**
   * 校验 itemEventSchema 数据
   */
  protected void assertIESData(Map<String, Object> data) {
    assertSchemaDataNode(data);
    assertTrue("数据节点中不存在 event 节点！", data.containsKey("event"));
    assertTrue("数据节点中不存在 time 节点！", data.containsKey("time"));
    assertEquals("数据节点个数不正常，可能丢失节点或多出来异常节点！", 8, data.size());
  }

  /**
   * 校验 itemSchema 数据
   */
  protected void assertISData(Map<String, Object> data) {
    assertSchemaDataNode(data);
    assertTrue("数据节点中不存在 id 节点！", data.containsKey("id"));
    assertNotNull("数据节点 id 不可为空！", data.get("id"));
    assertEquals("数据节点个数不正常，可能丢失节点或多出来异常节点！", 8, data.size());
  }

  /**
   * 校验 UserSchema 数据
   */
  protected void assertUSData(Map<String, Object> data) {
    assertSchemaDataNode(data);
    assertTrue("数据中用户信息节点（identities+distinct_id/user_id）丢失！",
        (data.containsKey("identities") && data.containsKey("distinct_id")) || data.containsKey("user_id"));
  }

  /**
   * 校验 UserItemSchema 数据
   */
  protected void assertUISData(Map<String, Object> data) {
    assertSchemaDataNode(data);
    assertTrue("数据节点中 id 不可为空！", data.containsKey("id"));
    Object properties = data.get("properties");
    assertTrue("数据节点 properties 类型不正确！", properties instanceof Map);
    Map<String, String> proMap = (Map<String, String>) properties;
    assertTrue("数据中用户信息节点（identities+distinct_id/user_id）丢失！",
        (proMap.containsKey("identities") && proMap.containsKey("distinct_id")) || proMap.containsKey("user_id"));
    assertTrue("数据节点 properties 内节点个数不正常！", proMap.size() >= 2);
  }

  protected void assertSchemaDataNode(Map<String, Object> data) {
    assertNotNull("数据节点为空！", data);
    assertTrue("数据节点中不存在 version 节点！", data.containsKey("version"));
    assertEquals("数据节点 version 值不正确！", SensorsConst.PROTOCOL_VERSION, data.get("version").toString());
    assertTrue("数据节点中不存在 type 节点！", data.containsKey("type"));
    assertTrue("数据节点中 type 值不正确！", actionTypeSet.contains(data.get("type").toString()));
    assertTrue("数据节点中不存在 schema 节点！", data.containsKey("schema"));
    assertTrue("数据节点中 schema 值不正确！", data.get("schema") instanceof String);
    assertTrue("数据节点中不存在 properties 节点！", data.containsKey("properties"));
    assertTrue("数据节点中不存在 track_id 节点！", data.containsKey("_track_id"));
    assertTrue("数据节点 track_id 值类型不正确！", data.get("_track_id") instanceof Integer);
    assertTrue("数据节点中不存在 lib 节点！", data.containsKey("lib"));
    assertTrue("数据节点中不存在 time 节点！", data.containsKey("time"));
    assertTrue("数据节点中 time 节点类型不正确！", data.get("time") instanceof Long);
    Object lib = data.get("lib");
    assertTrue("数据节点中 lib 节点类型不正确！", lib instanceof Map);
    assertEquals("数据节点 lib 内节点个数异常！", 4, ((Map<?, ?>) lib).size());
  }

}
