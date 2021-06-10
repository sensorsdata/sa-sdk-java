package com.sensorsdata.analytics.javasdk.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import java.io.IOException;
import java.io.Writer;
import java.util.Map;

public class ConsoleConsumer implements Consumer {
    private final ObjectMapper jsonMapper;
    private final Writer writer;

    public ConsoleConsumer(final Writer writer) {
        this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
        this.writer = writer;
    }

    @Override
    public void send(Map<String, Object> message) {
        try {
            synchronized (writer) {
                writer.write(jsonMapper.writeValueAsString(message));
                writer.write("\n");
            }
        } catch (IOException e) {
            throw new RuntimeException("Failed to dump message with ConsoleConsumer.", e);
        }
    }

    @Override
    public void flush() {
        synchronized (writer) {
            try {
                writer.flush();
            } catch (IOException e) {
                throw new RuntimeException("Failed to flush with ConsoleConsumer.", e);
            }
        }
    }

    @Override
    public void close() {
        flush();
    }
}