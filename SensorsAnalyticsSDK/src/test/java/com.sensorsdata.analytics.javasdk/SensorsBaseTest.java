package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_DETAIL_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_METHOD_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROPERTIES;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TIME_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ID;

import com.sensorsdata.analytics.javasdk.consumer.Consumer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import lombok.extern.slf4j.Slf4j;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@Slf4j
public class SensorsBaseTest {
    /** 单元测试中若生成多条数据，则存放到该集合中 */
    protected final List<Map<String, Object>> res = new ArrayList<>();
    /** SDK 最后一次生成的数据集合 */
    protected Map<String, Object> data = null;
    /** 初始化 sa */
    protected SensorsAnalytics sa = null;
    /** mock server */
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
        public void flush() {}

        @Override
        public void close() {}
    }

    @BeforeAll
    public static void mockABServer() throws Exception {
        log.info("Before All");
        server = new Server(8888);
        ServletContextHandler handler = new ServletContextHandler();
        handler.addServlet(new ServletHolder(new TestServlet()), "/debug");
        handler.addServlet(new ServletHolder(new TestServlet()), "/sa");
        handler.addServlet(new ServletHolder(new InstantServlet()), "/instant");

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
        actionTypeSet.add(SensorsConst.DETAIL_SET_ACTION_TYPE);
        actionTypeSet.add(SensorsConst.DETAIL_DELETE_ACTION_TYPE);
    }

    @Test
    public void checkServer() {
        Assertions.assertNotNull(server);
    }

    @AfterAll
    public static void closeMockServer() throws Exception {
        if (server != null) {
            server.stop();
        }
    }

    @BeforeEach
    public void clearListAndInit() {
        log.info("Before Each, new sa");
        sa = new SensorsAnalytics(new TestConsumer());
        data = null;
        res.clear();
    }

    /**
     * 校验非 IDM 模式下 event 数据节点 properties 节点可能没有数据，所以需放到具体的单元测试里面去判断
     *
     * @param data 数据节点
     */
    protected void assertEventData(Map<String, Object> data) {
        assertUserData(data);
        Assertions.assertTrue(data.containsKey("event"), "数据中没有 event 节点！");
    }

    /**
     * 校验非 IDM 模式下 user 数据节点
     *
     * @param data 数据节点
     */
    protected void assertUserData(Map<String, Object> data) {
        Assertions.assertNotNull(data);
        Assertions.assertTrue(data.containsKey("_track_id"), "数据中没有 _track_id 节点！");
        Assertions.assertTrue(data.containsKey("lib"), "数据中没有 lib 节点！");
        Assertions.assertTrue(data.containsKey("distinct_id"), "数据中没有 distinct_id 节点！");
        Assertions.assertTrue(data.containsKey("time"), "数据中没有 time 节点！");
        Assertions.assertTrue(data.containsKey("type"), "数据中没有 type 节点！");

        // 增加各字段类型校验
        Assertions.assertTrue(data.get("time") instanceof Long, "数据中 time 节点类型不正确！");
        Assertions.assertTrue(data.get("_track_id") instanceof Integer, "数据中 _track_id 节点类型不正确！");
        Assertions.assertTrue(
                data.get("distinct_id") instanceof String, "数据中 distinct_id 节点类型不正确！");
        Assertions.assertTrue(data.get("type") instanceof String, "数据中 type 节点类型不正确！");
        Assertions.assertTrue(data.get("lib") instanceof Map, "数据中 lib 节点类型不正确！");

        Assertions.assertFalse(data.containsKey("item_id"), "数据中包含 item_id 节点！");
        Assertions.assertFalse(data.containsKey("item_type"), "数据中包含 item_type 节点！");
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
        Assertions.assertNotNull(data);
        Assertions.assertTrue(data.containsKey("lib"), "item 数据中没有 lib 节点！");
        Assertions.assertTrue(data.containsKey("item_id"), "item 数据中没有 item_id 节点！");
        Assertions.assertTrue(data.containsKey("item_type"), "item 数据中没有 item_type 节点！");
        Assertions.assertTrue(data.containsKey("time"), "item 数据中没有 time 节点！");
        Assertions.assertTrue(data.containsKey("type"), "item 数据中没有 type 节点！");
        Assertions.assertFalse(data.containsKey("distinct_id"), "item 数据中包含 distinct_id 节点！");
        Assertions.assertFalse(data.containsKey("_track_id"), "item 数据中包含 _track_id 节点！");
    }

    /**
     * 校验 IDM3.0 模式下 event 数据节点
     *
     * @param data 数据节点
     */
    protected void assertIDM3EventData(Map<String, Object> data) {
        assertEventData(data);
        Assertions.assertTrue(data.containsKey("identities"), "IDM3 数据中没有 identities 节点！");
    }

    /**
     * 校验 IDM3.0 模式下 user 数据节点
     *
     * @param data 数据节点
     */
    protected void assertIDM3UserData(Map<String, Object> data) {
        assertUserData(data);
        Assertions.assertTrue(data.containsKey("identities"), "IDM3 数据中没有 identities 节点！");
    }

    /**
     * 校验 lib 内容
     *
     * @param lib lib 信息集合
     */
    protected void assertDataLib(Object lib) {
        Assertions.assertTrue(lib instanceof Map);
        Map<String, String> libMap = (Map<String, String>) lib;
        Assertions.assertFalse(libMap.isEmpty(), "数据中 lib 节点信息不存在！");
        Assertions.assertTrue(libMap.containsKey(LIB_SYSTEM_ATTR), "lib 节点信息不存在 $lib 信息！");
        Assertions.assertTrue(
                libMap.containsKey(LIB_VERSION_SYSTEM_ATTR), "lib 节点信息不存在 $lib_version 信息！");
        Assertions.assertTrue(
                libMap.containsKey(LIB_METHOD_SYSTEM_ATTR), "lib 节点信息不存在 $lib_method 信息！");
        Assertions.assertTrue(
                libMap.containsKey(LIB_DETAIL_SYSTEM_ATTR), "lib 节点信息不存在 $lib_detail 信息！");
    }

    /** 校验 properties 校验原则：如果 properties 存在，判断 $time 和 $track_id 是否存在 */
    protected void assertProperties(Object properties) {
        Assertions.assertTrue(properties instanceof Map);
        Map<String, Object> propertiesMap = (Map<String, Object>) properties;
        Assertions.assertFalse(
                propertiesMap.containsKey(TIME_SYSTEM_ATTR), "properties 中的 $time 属性并未删除");
        Assertions.assertFalse(
                propertiesMap.containsKey(TRACK_ID), "properties 中的 $track_id 属性并未删除");
    }

    /** 校验 userEventSchema 数据 */
    protected void assertUESData(Map<String, Object> data) {
        assertIESData(data);
        Object properties = data.get("properties");
        Assertions.assertTrue(properties instanceof Map, "数据节点 properties 类型不正确！");
        Map<String, String> proMap = (Map<String, String>) properties;
        Assertions.assertTrue(
                (proMap.containsKey("identities") || proMap.containsKey("user_id"))
                        && proMap.containsKey("distinct_id"),
                "数据中用户信息节点（identities+distinct_id/user_id）丢失！");
    }

    protected void assertEventIdentitiesInfo(Map<String, Object> data, String expectDistinctId) {
        Map<String, String> proMap = (Map<String, String>) data.get("properties");
        Assertions.assertTrue(proMap.containsKey("identities"));
        Assertions.assertTrue(proMap.containsKey("distinct_id"));
        Assertions.assertEquals(expectDistinctId, proMap.get("distinct_id").toString());
    }

    /** 校验 itemEventSchema 数据 */
    protected void assertIESData(Map<String, Object> data) {
        assertSchemaDataNode(data);
        Assertions.assertTrue(data.containsKey("event"), "数据节点中不存在 event 节点！");
        Assertions.assertTrue(data.containsKey("time"), "数据节点中不存在 time 节点！");
        Assertions.assertTrue(data.size() >= 8, "数据节点个数不正常，可能丢失节点！");
    }

    /** 校验 itemSchema 数据 */
    protected void assertISData(Map<String, Object> data) {
        assertSchemaDataNode(data);
        Assertions.assertTrue(data.containsKey("id"), "数据节点中不存在 id 节点！");
        Assertions.assertNotNull(data.get("id"), "数据节点 id 不可为空！");
        Assertions.assertTrue(data.size() >= 8, "数据节点个数不正常，可能丢失节点！");
    }

    /** 校验 UserSchema 数据 */
    protected void assertUSData(Map<String, Object> data) {
        assertSchemaDataNode(data);
        Assertions.assertTrue(
                (data.containsKey("identities") || data.containsKey("id"))
                        && data.containsKey("distinct_id"),
                "数据中用户信息节点（identities+distinct_id/user_id）丢失！");
    }

    /** 校验 UserItemSchema 数据 */
    protected void assertUISData(Map<String, Object> data) {
        assertSchemaDataNode(data);
        Assertions.assertTrue(data.containsKey("id"), "数据节点中 id 不可为空！");
        Object properties = data.get("properties");
        Assertions.assertTrue(properties instanceof Map, "数据节点 properties 类型不正确！");
        Map<String, String> proMap = (Map<String, String>) properties;
        Assertions.assertTrue(
                (proMap.containsKey("identities") || proMap.containsKey("user_id"))
                        && proMap.containsKey("distinct_id"),
                "数据中用户信息节点（identities+distinct_id/user_id）丢失！");
        Assertions.assertTrue(proMap.size() >= 2, "数据节点 properties 内节点个数不正常！");
    }

    protected void assertSchemaDataNode(Map<String, Object> data) {
        Assertions.assertNotNull(data, "数据节点为空！");
        Assertions.assertTrue(data.containsKey("version"), "数据节点中不存在 version 节点！");
        Assertions.assertEquals(
                SensorsConst.PROTOCOL_VERSION,
                data.get("version").toString(),
                "数据节点 version 值不正确！");
        Assertions.assertTrue(data.containsKey("type"), "数据节点中不存在 type 节点！");
        Assertions.assertTrue(
                actionTypeSet.contains(data.get("type").toString()), "数据节点中 type 值不正确！");
        Assertions.assertTrue(data.containsKey("schema"), "数据节点中不存在 schema 节点！");
        Assertions.assertTrue(data.get("schema") instanceof String, "数据节点中 schema 值不正确！");
        Assertions.assertTrue(data.containsKey("properties"), "数据节点中不存在 properties 节点！");
        Assertions.assertTrue(data.containsKey("_track_id"), "数据节点中不存在 track_id 节点！");
        Assertions.assertTrue(data.get("_track_id") instanceof Integer, "数据节点 track_id 值类型不正确！");
        Assertions.assertTrue(data.containsKey("lib"), "数据节点中不存在 lib 节点！");
        Assertions.assertTrue(data.containsKey("time"), "数据节点中不存在 time 节点！");
        Assertions.assertTrue(data.get("time") instanceof Long, "数据节点中 time 节点类型不正确！");
        Object lib = data.get("lib");
        Assertions.assertTrue(lib instanceof Map, "数据节点中 lib 节点类型不正确！");
        Assertions.assertTrue(((Map<?, ?>) lib).size() >= 4, "数据节点 lib 内节点个数异常！");
    }
}
