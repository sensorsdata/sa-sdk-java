package com.sensorsdata.analytics.javasdk.consumer;

import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Slf4j
public class BatchConsumer implements Consumer {
    private static final int MAX_FLUSH_BULK_SIZE = 1000;
    private static final int MAX_CACHE_SIZE = 6000;
    private static final int MIN_CACHE_SIZE = 3000;

    private final List<Map<String, Object>> messageList;
    private final HttpConsumer httpConsumer;
    private final InstantHttpConsumer instantHttpConsumer;
    private final ObjectMapper jsonMapper;
    private final int bulkSize;
    private final boolean throwException;
    private final int maxCacheSize;
    private List<String> instantEvents;
    private boolean isInstantStatus;

    public BatchConsumer(final String serverUrl) {
        this(serverUrl, 50);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize) {
        this(serverUrl, bulkSize, 3);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec) {
        this(serverUrl, bulkSize, false, timeoutSec);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final boolean throwException) {
        this(serverUrl, bulkSize, throwException, 3);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final boolean throwException,
        final int timeoutSec) {
        this(serverUrl, bulkSize, 0, throwException, timeoutSec);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final int maxCacheSize,
        final boolean throwException) {
        this(serverUrl, bulkSize, maxCacheSize, throwException, 3);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final int maxCacheSize,
                         final boolean throwException, final int timeoutSec) {
        this(HttpClients.custom(), serverUrl, bulkSize, maxCacheSize, throwException, timeoutSec);
    }

    public BatchConsumer(HttpClientBuilder httpClientBuilder, final String serverUrl, final int bulkSize, final int maxCacheSize,
                         final boolean throwException, final int timeoutSec) {
        this(httpClientBuilder, serverUrl, bulkSize, maxCacheSize, throwException, timeoutSec, new ArrayList<String>());
    }


    public BatchConsumer(final String serverUrl, final int bulkSize, final int maxCacheSize,
        final boolean throwException, final int timeoutSec, List<String> instantEvents) {
        this(HttpClients.custom(), serverUrl, bulkSize, maxCacheSize, throwException, timeoutSec, instantEvents);
    }

    public BatchConsumer(HttpClientBuilder httpClientBuilder, final String serverUrl, final int bulkSize, final int maxCacheSize,
        final boolean throwException, final int timeoutSec, List<String> instantEvents) {
        this.messageList = new LinkedList<>();
        this.isInstantStatus = false;
        this.instantEvents = instantEvents;
        this.httpConsumer = new HttpConsumer(httpClientBuilder, serverUrl, Math.max(timeoutSec, 1));
        this.instantHttpConsumer = new InstantHttpConsumer(httpClientBuilder, serverUrl, Math.max(timeoutSec, 1));
        this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
        this.bulkSize = Math.min(MAX_FLUSH_BULK_SIZE, Math.max(1, bulkSize));
        if (maxCacheSize > MAX_CACHE_SIZE) {
            this.maxCacheSize = MAX_CACHE_SIZE;
        } else if (maxCacheSize > 0 && maxCacheSize < MIN_CACHE_SIZE) {
            this.maxCacheSize = MIN_CACHE_SIZE;
        } else {
            this.maxCacheSize = maxCacheSize;
        }
        this.throwException = throwException;
        log.info(
            "Initialize BatchConsumer with params:[bulkSize:{},timeoutSec:{},maxCacheSize:{},throwException:{}]",
            bulkSize, timeoutSec, maxCacheSize, throwException);
    }

    @Override
    public void send(Map<String, Object> message) {
        synchronized (messageList) {
            dealInstantSignal(message);
            int size = messageList.size();
            if (maxCacheSize <= 0 || size < maxCacheSize) {
                messageList.add(message);
                ++size;
                log.info("Successfully save data to cache,The cache current size is {}.", size);
            }
            if (size >= bulkSize) {
                log.info("Flush was triggered because the cache size reached the threshold,cache size:{},bulkSize:{}.",
                    size, bulkSize);
                flush();
            }
        }
    }

    @Override
    public void flush() {
        synchronized (messageList) {
            while (!messageList.isEmpty()) {
                String sendingData;
                List<Map<String, Object>> sendList = messageList.subList(0, Math.min(bulkSize, messageList.size()));
                try {
                    sendingData = jsonMapper.writeValueAsString(sendList);
                } catch (JsonProcessingException e) {
                    sendList.clear();
                    log.error("Failed to process json.", e);
                    if (throwException) {
                        throw new RuntimeException("Failed to serialize data.", e);
                    }
                    continue;
                }
                log.debug("Will be send data:{}.", sendingData);
                try {
                    if (isInstantStatus) {
                        this.instantHttpConsumer.consume(sendingData);
                    } else {
                        this.httpConsumer.consume(sendingData);
                    }
                    sendList.clear();
                } catch (Exception e) {
                    log.error("Failed to send data:{}.", sendingData, e);
                    if (throwException) {
                        throw new RuntimeException("Failed to dump message with BatchConsumer.", e);
                    }
                    return;
                }
                log.debug("Successfully send data:{}.", sendingData);
            }
            log.info("Finish flush.");
        }
    }

    @Override
    public void close() {
        flush();
        httpConsumer.close();
        log.info("Call close method.");
    }

    private void dealInstantSignal(Map<String, Object> message) {

        /*
         * 如果当前是「instant」状态，且（message中不包含event 或者 event 不是「instant」的，则刷新，设置 「非instant」状态
         */
        if (isInstantStatus && (!message.containsKey("event") || !instantEvents.contains(message.get("event")))) {
            flush();
            isInstantStatus = false;
        }

        /*
         * 如果当前是 「非instant」状态，且（message中包含event 且 event 是「instant」的，则刷新，设置 「instant」状态
         */
        if (!isInstantStatus && message.containsKey("event") && instantEvents.contains(message.get("event"))) {
            flush();
            isInstantStatus = true;
        }
    }
}