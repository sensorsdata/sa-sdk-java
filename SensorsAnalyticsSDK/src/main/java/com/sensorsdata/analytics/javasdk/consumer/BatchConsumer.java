package com.sensorsdata.analytics.javasdk.consumer;

import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class BatchConsumer implements Consumer {
    private static final int MAX_FLUSH_BULK_SIZE = 1000;
    private static final int MAX_CACHE_SIZE = 6000;
    private static final int MIN_CACHE_SIZE = 3000;

    private final List<Map<String, Object>> messageList;
    private final HttpConsumer httpConsumer;
    private final ObjectMapper jsonMapper;
    private final int bulkSize;
    private final boolean throwException;
    private final int maxCacheSize;

    public BatchConsumer(final String serverUrl) {
        this(serverUrl, 50);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize) {
        this(serverUrl, 3, bulkSize);
    }

    public BatchConsumer(final String serverUrl, final int timeoutSec, final int bulkSize) {
        this(serverUrl, bulkSize, timeoutSec, false);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec,
        final boolean throwException) {
        this(serverUrl, bulkSize, timeoutSec, 0, throwException);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec, final int maxCacheSize,
        final boolean throwException) {
        this.messageList = new LinkedList<Map<String, Object>>();
        this.httpConsumer = new HttpConsumer(serverUrl, timeoutSec);
        this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
        this.bulkSize = Math.min(MAX_FLUSH_BULK_SIZE, bulkSize);
        if (maxCacheSize > MAX_CACHE_SIZE) {
            this.maxCacheSize = MAX_CACHE_SIZE;
        } else if (maxCacheSize > 0 && maxCacheSize < MIN_CACHE_SIZE) {
            this.maxCacheSize = MIN_CACHE_SIZE;
        } else {
            this.maxCacheSize = maxCacheSize;
        }
        this.throwException = throwException;
    }

    @Override
    public void send(Map<String, Object> message) {
        synchronized (messageList) {
            int size = messageList.size();
            if (maxCacheSize <= 0 || size < maxCacheSize) {
                messageList.add(message);
                ++size;
            }
            if (size >= bulkSize) {
                flush();
            }
        }
    }

    @Override
    public void flush() {
        synchronized (messageList) {
            while (!messageList.isEmpty()) {
                String sendingData = null;
                List<Map<String, Object>> sendList =
                        messageList.subList(0, Math.min(bulkSize, messageList.size()));
                try {
                    sendingData = jsonMapper.writeValueAsString(sendList);
                } catch (JsonProcessingException e) {
                    sendList.clear();
                    if (throwException) {
                        throw new RuntimeException("Failed to serialize data.", e);
                    }
                    continue;
                }

                try {
                    this.httpConsumer.consume(sendingData);
                    sendList.clear();
                } catch (Exception e) {
                    if (throwException) {
                        throw new RuntimeException("Failed to dump message with BatchConsumer.", e);
                    }
                    return;
                }
            }
        }
    }

    @Override
    public void close() {
        flush();
        httpConsumer.close();
    }
}