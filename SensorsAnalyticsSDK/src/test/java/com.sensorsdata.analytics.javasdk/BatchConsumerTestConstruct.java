package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * BatchConsumer 单测
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/25 15:09
 */
public class BatchConsumerTestConstruct {

  private BatchConsumer batchConsumer;
  private SensorsAnalytics sa;
  private List<Map<String, Object>> messageList;
  private String serverUrl = "http://10.120.73.51:8106/sa?project=default&token=";

  @Before
  public void init() throws NoSuchFieldException, IllegalAccessException {
//    batchConsumer = new BatchConsumer("http://localhost:8016/sa", 1, 3, true);
//    sa = new SensorsAnalytics(batchConsumer);
//    Field field = batchConsumer.getClass().getDeclaredField("messageList");
//    field.setAccessible(true);
//    messageList = (List<Map<String, Object>>) field.get(batchConsumer);
  }


  private void assertBlukSize(int bulkSizeExp) throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException{
    Field field = batchConsumer.getClass().getDeclaredField("messageList");
    field.setAccessible(true);
    messageList = (List<Map<String, Object>>) field.get(batchConsumer);

    field = batchConsumer.getClass().getDeclaredField("bulkSize");
    field.setAccessible(true);
    int bulkSizeRel = (Integer) field.get(batchConsumer);

    assertEquals(bulkSizeExp, bulkSizeRel);

    for (int i = 0; i < (bulkSizeExp-1); i++){
      sa.track("123", true, "test" + i, null);
    }
    assertEquals(bulkSizeExp-1, messageList.size());
    Thread.sleep(3000);
    sa.track("123", true, "test" + (bulkSizeExp-1), null);
    Thread.sleep(3000);
    assertEquals(0, messageList.size());
  }

  private void assertMaxCacheSize(int maxCacheSizeExp) throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException{
    Field field = batchConsumer.getClass().getDeclaredField("messageList");
    field.setAccessible(true);
    messageList = (List<Map<String, Object>>) field.get(batchConsumer);

    field = batchConsumer.getClass().getDeclaredField("maxCacheSize");
    field.setAccessible(true);
    int maxCacheSizeRel = (Integer) field.get(batchConsumer);

    assertEquals(maxCacheSizeExp, maxCacheSizeRel);

    if(maxCacheSizeExp != 0){
      for (int i = 0; i < (maxCacheSizeExp); i++){
        sa.track("123", true, "test" + i, null);
      }
//      assertEquals(maxCacheSizeExp, messageList.size());
      assertTrue(maxCacheSizeExp > messageList.size());
    }
  }

  private void assertThrowException(Boolean throwExceptionExp) throws NoSuchFieldException, IllegalAccessException {
    Field field = batchConsumer.getClass().getDeclaredField("throwException");
    field.setAccessible(true);
    Boolean throwExceptionRel = (Boolean) field.get(batchConsumer);

    assertEquals(throwExceptionExp, throwExceptionRel);
  }


  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(final String serverUrl)
   */
  @Test
  public void testBatchConsumer01() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 50;
    int maxCacheSize = 0;
    boolean throwException = false;

    batchConsumer = new BatchConsumer(serverUrl);
    sa = new SensorsAnalytics(batchConsumer);

    assertBlukSize(bulkSize);

    serverUrl = "http://localhost:8887/test";
    batchConsumer = new BatchConsumer(serverUrl);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(final String serverUrl, final int bulkSize)
   */
  @Test
  public void testBatchConsumer02() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 100;
    int maxCacheSize = 0;
    boolean throwException = false;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize);
    sa = new SensorsAnalytics(batchConsumer);

    assertBlukSize(bulkSize);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(final String serverUrl, final int bulkSize)
   */
  @Test
  public void testBatchConsumer02BlukSize() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 100;
    int maxCacheSize = 0;
    boolean throwException = false;

    batchConsumer = new BatchConsumer(serverUrl, 0);
    sa = new SensorsAnalytics(batchConsumer);

    assertBlukSize(1);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(final String serverUrl, final int bulkSize, final boolean throwException)
   */
  @Test
  public void testBatchConsumer03() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 100;
    int maxCacheSize = 0;
    boolean throwException = true;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, throwException);
    sa = new SensorsAnalytics(batchConsumer);

    assertBlukSize(bulkSize);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

    /**
     * 测试 BatchConsumer 构造函数
     * public BatchConsumer(final String serverUrl, final int bulkSize, final int maxCacheSize, final boolean throwException)
   */
  @Test
  public void testBatchConsumer04() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 900;
    int maxCacheSize = 4000;
    boolean throwException = true;

    batchConsumer = new BatchConsumer(serverUrl,bulkSize,maxCacheSize, throwException);
    sa = new SensorsAnalytics(batchConsumer);

    assertBlukSize(bulkSize);

    serverUrl = "http://localhost:8887/test";
    batchConsumer = new BatchConsumer(serverUrl, bulkSize, maxCacheSize, throwException);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec)
   */
  @Test
  public void testBatchConsumerTimeoutSec05() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 100;
    int maxCacheSize = 0;
    int timeoutSec = 10;
    boolean throwException = false;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, timeoutSec);
    sa = new SensorsAnalytics(batchConsumer);

    assertBlukSize(bulkSize);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数 timeoutSec 参数，需要使用 Charles 配合测试
   * public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec)
   */
  @Test
  public void testBatchConsumerTimeoutSec05AssertTimeout() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    String serverUrl = "http://localhost:8887/test";
    int bulkSize = 100;
    int timeoutSec = 5;
    long start = 0,end;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, timeoutSec);
    sa = new SensorsAnalytics(batchConsumer);
//    try {
      start = System.currentTimeMillis();
      sa.track("xc11", true, "Exent1");
      sa.flush();
//    }catch (Exception e){
//      end = System.currentTimeMillis();
//      assertTrue((end-start) < timeoutSec*1000+200 && (end-start) > timeoutSec*1000-200);
//      System.out.println("==========");
//      e.printStackTrace();
//    }
  }

  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(String serverUrl, int bulkSize, int maxCacheSize, boolean throwException, int timeoutSec)
   */
  @Test
  public void testBatchConsumerTimeoutSec06() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 100;
    int timeoutSec = 5;
    int maxCacheSize = 0;
    boolean throwException = true;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, throwException, timeoutSec);
    sa = new SensorsAnalytics(batchConsumer);
    assertBlukSize(bulkSize);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数 timeoutSec 参数，需要使用 Charles 匹配
   * public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec)
   */
  @Test
  public void testBatchConsumerTimeoutSec06AssertTimeout() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    String serverUrl = "http://localhost:8887/test";
    int bulkSize = 100;
    int timeoutSec = 5;
    int maxCacheSize = 4000;
    boolean throwException = true;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, throwException, timeoutSec);
    sa = new SensorsAnalytics(batchConsumer);
    sa.track("xc11", true, "Exent1");
    sa.flush();
//    Thread.sleep(15*1000);
  }

  /**
   * 测试 BatchConsumer 构造函数
   * public BatchConsumer(String serverUrl, int bulkSize, int maxCacheSize, boolean throwException, int timeoutSec)
   */
  @Test
  public void testBatchConsumerTimeoutSec07() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    int bulkSize = 100;
    int timeoutSec = 5;
    int maxCacheSize = 4000;
    boolean throwException = true;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, maxCacheSize, throwException, timeoutSec);
    sa = new SensorsAnalytics(batchConsumer);
    assertBlukSize(bulkSize);
    assertMaxCacheSize(maxCacheSize);
    assertThrowException(throwException);
  }

  /**
   * 测试 BatchConsumer 构造函数 timeoutSec 参数，需要使用 Charles 匹配
   * public BatchConsumer(final String serverUrl, final int bulkSize, final int timeoutSec)
   */
  @Test
  public void testBatchConsumerTimeoutSec07AssertTimeout() throws NoSuchFieldException, IllegalAccessException, InterruptedException, InvalidArgumentException {
    String serverUrl = "http://localhost:8887/test";
    int bulkSize = 100;
    int timeoutSec = 5;
    int maxCacheSize = 4000;
    boolean throwException = true;

    batchConsumer = new BatchConsumer(serverUrl, bulkSize, maxCacheSize, throwException, timeoutSec);
    sa = new SensorsAnalytics(batchConsumer);
    sa.track("xc11", true, "Exent1");
    sa.flush();
    Thread.sleep(15*1000);
  }



}
