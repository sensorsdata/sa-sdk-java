package com.sensorsdata.analytics.javasdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Base64;
import java.util.zip.GZIPInputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.junit.jupiter.api.Assertions;

/**
 * 模拟服务端接收数据
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/01/18 18:33
 */
public class TestServlet extends HttpServlet {

    @Override
    protected void doPost(HttpServletRequest request, HttpServletResponse response)
            throws IOException {
        String gzip = request.getParameter("gzip");
        Assertions.assertEquals("1", gzip);
        String dataList = request.getParameter("data_list");
        byte[] bytes = Base64.getDecoder().decode(dataList);
        byte[] data = decompressGzip(bytes);
        ArrayNode arrayNode = (ArrayNode) SensorsAnalyticsUtil.getJsonObjectMapper().readTree(data);
        for (JsonNode jsonNode : arrayNode) {
            Assertions.assertNotNull(jsonNode, "数据为空！");
            Assertions.assertTrue(jsonNode.has("type"), "数据中没有 type 节点！");
            Assertions.assertTrue(jsonNode.has("event"), "数据中没有 actionType 节点！");
            if (jsonNode.get("event").asText().startsWith("item")) {
                Assertions.assertTrue(jsonNode.has("item_id"), "item 数据没有 item_id 节点！");
                Assertions.assertTrue(jsonNode.has("item_type"), "item 数据没有 item_type 节点！");
            } else {
                Assertions.assertTrue(
                        jsonNode.has("_track_id"), "event or profile 数据没有 _track_id 节点！");
                Assertions.assertTrue(jsonNode.has("lib"), "event or profile 数据没有 lib 节点！");
                Assertions.assertTrue(jsonNode.has("time"), "event or profile 数据没有 time 节点！");
                Assertions.assertTrue(
                        jsonNode.has("distinct_id"), "event or profile 数据没有 distinct_id 节点！");
            }
        }
        response.setStatus(200);
    }

    protected byte[] decompressGzip(byte[] gzipData) throws IOException {
        byte[] bytes1 = new byte[1024];
        GZIPInputStream gis = new GZIPInputStream(new ByteArrayInputStream(gzipData));
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        int n;
        while ((n = gis.read(bytes1)) != -1) {
            bos.write(bytes1, 0, n);
        }
        bos.close();
        return bos.toByteArray();
    }
}
