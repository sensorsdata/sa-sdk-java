package com.sensorsdata.analytics.javasdk.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sensorsdata.analytics.javasdk.exceptions.DebugModeException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import org.apache.http.client.utils.URIBuilder;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class DebugConsumer implements Consumer {
    final HttpConsumer httpConsumer;
    final ObjectMapper jsonMapper;

    public DebugConsumer(final String serverUrl, final boolean writeData) {
        String debugUrl = null;
        try {
            // 将 URI Path 替换成 Debug 模式的 '/debug'
            URIBuilder builder = new URIBuilder(new URI(serverUrl));
            String[] urlPathes = builder.getPath().split("/");
            urlPathes[urlPathes.length - 1] = "debug";
            builder.setPath(SensorsAnalyticsUtil.strJoin(urlPathes, "/"));
            debugUrl = builder.build().toURL().toString();
        } catch (URISyntaxException e) {
            throw new DebugModeException(e);
        } catch (MalformedURLException e) {
            throw new DebugModeException(e);
        }

        Map<String, String> headers = new HashMap<String, String>();
        if (!writeData) {
            headers.put("Dry-Run", "true");
        }

        this.httpConsumer = new HttpConsumer(debugUrl, headers);
        this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
    }

    @Override
    public void send(Map<String, Object> message) {
        // XXX: HttpConsumer 只处理了 Message List 的发送？
        List<Map<String, Object>> messageList = new ArrayList<Map<String, Object>>();
        messageList.add(message);

        String sendingData = null;
        try {
            sendingData = jsonMapper.writeValueAsString(messageList);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize data.", e);
        }

        System.out
                .println("==========================================================================");

        try {
            synchronized (httpConsumer) {
                httpConsumer.consume(sendingData);
            }

            System.out.println(String.format("valid message: %s", sendingData));
        } catch (IOException e) {
            throw new DebugModeException("Failed to send message with DebugConsumer.", e);
        } catch (HttpConsumer.HttpConsumerException e) {
            System.out.println(String.format("invalid message: %s", e.getSendingData()));
            System.out.println(String.format("http status code: %d", e.getHttpStatusCode()));
            System.out.println(String.format("http content: %s", e.getHttpContent()));
            throw new DebugModeException(e);
        }
    }

    @Override
    public void flush() {
        // do NOTHING
    }

    @Override
    public void close() {
        httpConsumer.close();
    }
}
