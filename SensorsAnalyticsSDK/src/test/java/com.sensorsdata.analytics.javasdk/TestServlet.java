package com.sensorsdata.analytics.javasdk;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import sun.misc.BASE64Decoder;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import static org.junit.Assert.*;

/**
 * 模拟服务端接收数据
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/01/18 18:33
 */
public class TestServlet extends HttpServlet {

  @Override
  protected void doPost(HttpServletRequest request, HttpServletResponse response) throws IOException {
    String gzip = request.getParameter("gzip");
    assertEquals("1", gzip);
    String dataList = request.getParameter("data_list");
    BASE64Decoder base64Decoder = new BASE64Decoder();
    byte[] bytes = base64Decoder.decodeBuffer(dataList);
    byte[] data = decompressGzip(bytes);
    ArrayNode arrayNode = (ArrayNode) SensorsAnalyticsUtil.getJsonObjectMapper().readTree(data);
    for (JsonNode jsonNode : arrayNode) {
      assertNotNull("数据为空！", jsonNode);
      assertTrue("数据中没有 type 节点！", jsonNode.has("type"));
      assertTrue("数据中没有 actionType 节点！", jsonNode.has("event"));
      if (jsonNode.get("event").asText().startsWith("item")) {
        assertTrue("item 数据没有 item_id 节点！", jsonNode.has("item_id"));
        assertTrue("item 数据没有 item_type 节点！", jsonNode.has("item_type"));
      } else {
        assertTrue("event or profile 数据没有 _track_id 节点！", jsonNode.has("_track_id"));
        assertTrue("event or profile 数据没有 lib 节点！", jsonNode.has("lib"));
        assertTrue("event or profile 数据没有 time 节点！", jsonNode.has("time"));
        assertTrue("event or profile 数据没有 distinct_id 节点！", jsonNode.has("distinct_id"));
      }
    }
    response.setStatus(200);
  }

  private byte[] decompressGzip(byte[] gzipData) throws IOException {
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
