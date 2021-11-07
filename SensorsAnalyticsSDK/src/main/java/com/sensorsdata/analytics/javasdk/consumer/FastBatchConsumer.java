package com.sensorsdata.analytics.javasdk.consumer;

import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * 网络批量请求发送，异常快速返回模式
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/05 23:48
 */
public class FastBatchConsumer implements Consumer {

  private static final int MAX_CACHE_SIZE = 10000;
  private static final int MIN_CACHE_SIZE = 1000;

  private final LinkedBlockingQueue<Map<String, Object>> buffer;
  private final HttpConsumer httpConsumer;
  private final ObjectMapper jsonMapper;
  private final Callback callback;
  private final int bulkSize;
  private final ScheduledExecutorService executorService;

  public FastBatchConsumer(@NonNull String serverUrl, @NonNull Callback callback) {
    this(serverUrl, true, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, @NonNull Callback callback) {
    this(serverUrl, timing, 50, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, int bulkSize, @NonNull Callback callback) {
    this(serverUrl, timing, bulkSize, 6000, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, int bulkSize, int maxCacheSize,
      @NonNull Callback callback) {
    this(serverUrl, timing, bulkSize, maxCacheSize, 3, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, final int bulkSize, int maxCacheSize,
      int timeoutSec, @NonNull Callback callback) {
    this.buffer =
        new LinkedBlockingQueue<Map<String, Object>>(Math.min(Math.max(MIN_CACHE_SIZE, maxCacheSize), MAX_CACHE_SIZE));
    this.httpConsumer = new HttpConsumer(serverUrl, timeoutSec);
    this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
    this.callback = callback;
    this.bulkSize = bulkSize;
    executorService = new ScheduledThreadPoolExecutor(1);
    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (timing) {
          if (buffer.size() >= bulkSize) {
            flush();
          }
        } else {
          flush();
        }
      }
    }, 200, 20, TimeUnit.MILLISECONDS);
  }

  @Override
  public void send(Map<String, Object> message) {
    if (!buffer.offer(message)) {
      callback.onFailed(new FailedData("can't offer to buffer.", message));
    }
  }

  /**
   * This method don't need to be called actively.Because instance will create scheduled thread to do.
   */
  @Override
  public void flush() {
    List<Map<String, Object>> results = new ArrayList<Map<String, Object>>();
    buffer.drainTo(results);
    if (results.isEmpty()) {
      return;
    }
    while (!results.isEmpty()) {
      String sendingData;
      List<Map<String, Object>> sendList = results.subList(0, Math.min(bulkSize, results.size()));
      try {
        sendingData = jsonMapper.writeValueAsString(sendList);
      } catch (JsonProcessingException e) {
        callback.onFailed(new FailedData("can't process json.", sendList));
        sendList.clear();
        return;
      }
      try {
        this.httpConsumer.consume(sendingData);
      } catch (Exception e) {
        callback.onFailed(new FailedData("failed to send data.", sendingData));
        sendList.clear();
      }
    }
  }

  @Override
  public void close() {
    this.httpConsumer.close();
    this.executorService.shutdown();
  }
}
