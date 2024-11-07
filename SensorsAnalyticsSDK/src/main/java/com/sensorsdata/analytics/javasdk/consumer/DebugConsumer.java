package com.sensorsdata.analytics.javasdk.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sensorsdata.analytics.javasdk.exceptions.DebugModeException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

@Slf4j
public class DebugConsumer implements Consumer {
    final HttpConsumer httpConsumer;
    final ObjectMapper jsonMapper;

    public DebugConsumer(final String serverUrl, final boolean writeData) {
        this(HttpClients.custom(), serverUrl, writeData);
    }

    public DebugConsumer(
            HttpClientBuilder httpClientBuilder, String serverUrl, final boolean writeData) {
        String debugUrl;
        try {
            // 将 URI Path 替换成 Debug 模式的 '/debug'
            URIBuilder builder = new URIBuilder(new URI(serverUrl));
            String[] urlPathes = builder.getPath().split("/");
            urlPathes[urlPathes.length - 1] = "debug";
            builder.setPath(SensorsAnalyticsUtil.strJoin(urlPathes, "/"));
            debugUrl = builder.build().toURL().toString();
        } catch (URISyntaxException | MalformedURLException e) {
            log.error("Failed build debug url:[{}].", serverUrl, e);
            throw new DebugModeException(e);
        }

        Map<String, String> headers = new HashMap<>();
        if (!writeData) {
            headers.put("Dry-Run", "true");
        }
        this.httpConsumer = new HttpConsumer(httpClientBuilder, debugUrl, headers);
        this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
        log.info("Initialize DebugConsumer with params:[writeData:{}].", writeData);
    }

    @Override
    public void send(Map<String, Object> message) {
        List<Map<String, Object>> messageList = new ArrayList<>();
        messageList.add(message);
        String sendingData;
        try {
            sendingData = jsonMapper.writeValueAsString(messageList);
        } catch (JsonProcessingException e) {
            log.error("Failed to process json.", e);
            throw new RuntimeException("Failed to serialize data.", e);
        }
        try {
            synchronized (httpConsumer) {
                httpConsumer.consume(sendingData);
            }
            log.debug("Successfully send data:[{}].", sendingData);
        } catch (IOException e) {
            log.error("Failed to send message with DebugConsumer,message:[{}].", sendingData, e);
            throw new DebugModeException("Failed to send message with DebugConsumer.", e);
        } catch (HttpConsumer.HttpConsumerException e) {
            log.error("Failed send message with server occur error,message:[{}].", sendingData, e);
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
