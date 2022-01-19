package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import sun.misc.BASE64Decoder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

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
      assertNotNull(jsonNode);
      assertTrue(jsonNode.has("_track_id"));
      assertTrue(jsonNode.has("lib"));
      assertTrue(jsonNode.has("time"));
      assertTrue(jsonNode.has("distinct_id"));
      assertTrue(jsonNode.has("type"));
      assertTrue(jsonNode.has("event"));
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
