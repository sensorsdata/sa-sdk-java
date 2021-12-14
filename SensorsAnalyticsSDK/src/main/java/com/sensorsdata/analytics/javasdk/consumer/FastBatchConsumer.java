package com.sensorsdata.analytics.javasdk.consumer;

import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

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
@Slf4j
public class FastBatchConsumer implements Consumer {

  private static final int MAX_CACHE_SIZE = 10000;
  private static final int MIN_CACHE_SIZE = 1000;
  private static final int MIN_BULK_SIZE = 1;


  private final LinkedBlockingQueue<Map<String, Object>> buffer;
  private final HttpConsumer httpConsumer;
  private final ObjectMapper jsonMapper;
  private final Callback callback;
  private final int bulkSize;
  private final ScheduledExecutorService executorService;

  public FastBatchConsumer(@NonNull String serverUrl, @NonNull Callback callback) {
    this(serverUrl, false, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, int flushSec, final boolean timing, @NonNull Callback callback) {
    this(serverUrl, timing, 50, 6000, flushSec, 3, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, @NonNull Callback callback) {
    this(serverUrl, timing, 50, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, int bulkSize, @NonNull Callback callback) {
    this(serverUrl, timing, bulkSize, 6000, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, int bulkSize, int maxCacheSize,
      @NonNull Callback callback) {
    this(serverUrl, timing, bulkSize, maxCacheSize, 1, 3, callback);
  }

  public FastBatchConsumer(@NonNull String serverUrl, final boolean timing, final int bulkSize, int maxCacheSize,
      int flushSec, int timeoutSec, @NonNull Callback callback) {
    this.buffer =
        new LinkedBlockingQueue<>(Math.min(Math.max(MIN_CACHE_SIZE, maxCacheSize), MAX_CACHE_SIZE));
    this.httpConsumer = new HttpConsumer(serverUrl, Math.max(timeoutSec, 1));
    this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
    this.callback = callback;
    this.bulkSize = Math.min(MIN_CACHE_SIZE, Math.max(bulkSize, MIN_BULK_SIZE));
    executorService = new ScheduledThreadPoolExecutor(1);
    executorService.scheduleWithFixedDelay(new Runnable() {
      @Override
      public void run() {
        if (timing) {
          flush();
        } else {
          if (buffer.size() >= bulkSize) {
            flush();
          }
        }
      }
    }, 1, Math.max(flushSec, 1), TimeUnit.SECONDS);
    log.info(
        "Initialize FastBatchConsumer with params:[timing:{};bulkSize:{};maxCacheSize:{};flushSec:{};timeoutSec:{}].",
        timing, bulkSize, maxCacheSize, flushSec, timeoutSec);
  }

  @Override
  public void send(Map<String, Object> message) {
    if (!buffer.offer(message)) {
      List<Map<String, Object>> res = new ArrayList<>(1);
      res.add(message);
      callback.onFailed(new FailedData("can't offer to buffer.", res));
      log.error("Failed to load data into the cache.The cache current size is {}.", buffer.size());
    }
    log.info("Successfully save data to cache.The cache current size is {}.", buffer.size());
  }

  /**
   * This method don't need to be called actively.Because instance will create scheduled thread to do.
   */
  @Override
  public void flush() {
    List<Map<String, Object>> results = new ArrayList<>();
    buffer.drainTo(results);
    if (results.isEmpty()) {
      log.info("The Data of cache is empty when flush.");
      return;
    }
    log.info("Successfully get [{}] messages from the cache.", results.size());
    while (!results.isEmpty()) {
      String sendingData;
      List<Map<String, Object>> sendList = results.subList(0, Math.min(bulkSize, results.size()));
      try {
        sendingData = jsonMapper.writeValueAsString(sendList);
      } catch (JsonProcessingException e) {
        callback.onFailed(new FailedData(String.format("can't process json,message:%s.", e.getMessage()),
            SensorsAnalyticsUtil.deepCopy(results)));
        sendList.clear();
        log.error("Failed to process json.", e);
        continue;
      }
      log.debug("Data will be sent.{}", sendingData);
      try {
        this.httpConsumer.consume(sendingData);
      } catch (Exception e) {
        log.error("Failed to send data:{}.", sendingData, e);
        callback.onFailed(new FailedData(String.format("failed to send data,message:%s.", e.getMessage()),
            SensorsAnalyticsUtil.deepCopy(results)));
      }
      sendList.clear();
    }
    log.info("Finish flush.");
  }

  @Override
  public void close() {
    log.info("Call close method.");
    this.httpConsumer.close();
    this.executorService.shutdown();
  }

  /**
   * 重发送 FastBatchConsumer 模式发送失败返回的数据
   *
   * @param failedData 失败的数据集合
   * @return true:发送成功；false:发送失败
   */
  public boolean resendFailedData(@NonNull FailedData failedData)
      throws InvalidArgumentException, JsonProcessingException {
    SensorsAnalyticsUtil.assertFailedData(failedData);
    final String sendData = jsonMapper.writeValueAsString(failedData.getFailedData());
    log.debug("Will be resent data.{}", sendData);
    try {
      this.httpConsumer.consume(sendData);
    } catch (Exception e) {
      log.error("failed to send data.data:{}.", sendData, e);
      return false;
    }
    log.info("Successfully resend failed data.");
    return true;
  }
}
