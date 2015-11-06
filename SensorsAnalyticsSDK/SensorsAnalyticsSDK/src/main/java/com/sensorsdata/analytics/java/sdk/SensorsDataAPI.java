package com.sensorsdata.analytics.java.sdk;

import com.sensorsdata.analytics.java.sdk.util.Base64Coder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * SensorsData Analytics API
 * 提供各种日志记录、用户属性记录的功能
 */
public class SensorsDataAPI {

  private final static Logger log = LoggerFactory.getLogger(SensorsDataAPI.class);
  private final static int EXECUTE_THREAD_NUMBER = 1;
  private final static int EXECUTE_THREAD_SUBMMIT_TTL = 3 * 1000;

  private final static Pattern KEY_PATTERN = Pattern.compile("^[a-zA-Z_$][a-zA-Z\\d_$]*$");

  private final String eventsEndPoint;
  private final int flushInterval;
  private final int bulkUploadLimit;
  private final boolean sendingGZipContent;
  private final boolean debugMode;

  private final ThreadPoolExecutor executor;
  private final ThreadPoolExecutor timerExecutor;
  private long lastFlushTime;
  private Map<String, Object> superProperties;
  private List<Map<String, Object>> taskObjectList;

  private ObjectMapper jsonObjectMapper;

  /**
   * 构造函数
   *
   * @param serverUrl     接收日志的服务器地址
   * @param flushInterval 日志发送的最大时间间隔，单位毫秒
   * @param bulkSize      日志缓存的最大值，超过此值将立即进行发送
   * @param debugMode     Debug模式，该模式下会输出调试日志
   */
  public SensorsDataAPI(
      String serverUrl,
      int flushInterval,
      int bulkSize,
      boolean debugMode) {
    this.eventsEndPoint = serverUrl;
    this.flushInterval = flushInterval;
    this.bulkUploadLimit = bulkSize;
    this.sendingGZipContent = false;
    this.debugMode = debugMode;

    clearSuperProperties();

    this.taskObjectList = new ArrayList<Map<String, Object>>();

    this.lastFlushTime = System.currentTimeMillis();

    this.executor = new ThreadPoolExecutor(EXECUTE_THREAD_NUMBER, EXECUTE_THREAD_NUMBER,
        EXECUTE_THREAD_SUBMMIT_TTL, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        new LowPriorityThreadFactory());

    this.timerExecutor = new ThreadPoolExecutor(EXECUTE_THREAD_NUMBER, EXECUTE_THREAD_NUMBER,
        EXECUTE_THREAD_SUBMMIT_TTL, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
        new LowPriorityThreadFactory());
    this.timerExecutor.execute(new FlushTask());

    this.jsonObjectMapper = new ObjectMapper();
    // 容忍json中出现未知的列
    this.jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // 兼容java中的驼峰的字段名命名
    this.jsonObjectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
  }

  /**
   * 设置长期属性，所有track接口记录的事件都会拥有该属性
   *
   * @param superPropertiesMap  事件的一个或多个属性
   */
  public void registerSuperProperties(Map<String, Object> superPropertiesMap) {
    for (Map.Entry<String, Object> item : superPropertiesMap.entrySet()) {
      this.superProperties.put(item.getKey(), item.getValue());
    }
  }

  /**
   * 清除长期属性
   */
  public void clearSuperProperties() {
    this.superProperties = new HashMap<String, Object>();
    this.superProperties.put("$lib", "java");
  }

  /**
   * 记录一个没有属性的事件
   *
   * @param distinctId  用户ID
   * @param eventName   事件名称
   */
  public void track(String distinctId, String eventName) throws SensorsDataException {
    checkKey(distinctId);
    checkKey(eventName);
    executor.submit(new EventsQueueTask(distinctId, null, eventName, null));
  }

  /**
   * 记录一个拥有一个或多个属性的事件。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和
   * {@link List<?>}，若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * @param distinctId    用户ID
   * @param eventName     事件名称
   * @param properties    事件的属性
   */
  public void track(String distinctId, String eventName, Map<String, Object> properties)
      throws SensorsDataException {
    checkKey(distinctId);
    checkKey(eventName);
    checkTypeInProperties(properties);
    executor.submit(new EventsQueueTask(distinctId, null, eventName, properties));
  }

  /**
   * 用于记录用户登陆事件，将两个ID在数据分析系统中关联
   *
   * @param distinctId        新的用户ID
   * @param originDistinctId  旧的用户ID
   */
  public void trackSignUp(String distinctId, String originDistinctId) throws SensorsDataException {
    checkKey(distinctId);
    checkKey(originDistinctId);
    executor.submit(new EventsQueueTask(distinctId, originDistinctId, "$SignUp", null));
  }

  /**
   * 用于记录用户登陆事件，将两个ID在数据分析系统中关联，并设置一个或多个事件属性。属性取值可接受类型为
   * {@link Number}, {@link String}, {@link Date}和{@linkList<?>}，若属性包含 $time 字段，则
   * 它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * @param distinctId       新的用户ID
   * @param originDistinctId 旧的用户ID
   * @param properties       事件的属性
   */
  public void trackSignUp(String distinctId, String originDistinctId,
      Map<String, Object> properties) throws SensorsDataException {
    checkKey(distinctId);
    checkKey(originDistinctId);
    checkTypeInProperties(properties);
    executor.submit(new EventsQueueTask(distinctId, originDistinctId, "$SignUp", properties));
  }

  /**
   * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@linkList<?>}，
   * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
   *
   * @param distinctId    用户ID
   * @param properties    用户的属性
   */
  public void profileSet(String distinctId, Map<String, Object> properties)
      throws SensorsDataException {
    checkKey(distinctId);
    checkTypeInProperties(properties);
    executor.submit(new PeopleQueueTask(distinctId, "profile_set", properties));
  }

  /**
   * 设置用户的属性。这个接口只能设置单个key对应的内容，同样，如果已经存在，则覆盖，否则，新创建
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   * @param value         属性的值
   */
  public void profileSet(String distinctId, String property, Object value)
      throws SensorsDataException {
    checkKey(distinctId);
    checkValue(property, value);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    executor.submit(new PeopleQueueTask(distinctId, "profile_set", properties));
  }

  /**
   * 首次设置用户的属性。
   * 属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@linkList<?>}，
   * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * 与profileSet接口不同的是：
   *    如果要设置的properties的key，在这个用户的profile中已经存在，则不处理，否则，新创建
   *
   * @param distinctId    用户ID
   * @param properties    用户的属性
   */
  public void profileSetOnce(String distinctId, Map<String, Object> properties)
      throws SensorsDataException {
    checkKey(distinctId);
    checkTypeInProperties(properties);
    executor.submit(new PeopleQueueTask(distinctId, "profile_set_once", properties));
  }

  /**
   * 首次设置用户的属性。这个接口只能设置单个key对应的内容。
   * 与profileSet接口不同的是，如果key的内容之前已经存在，则不处理，否则，重新创建
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   * @param value         属性的值
   */
  public void profileSetOnce(String distinctId, String property, Object value)
      throws SensorsDataException {
    checkKey(distinctId);
    checkValue(property, value);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    executor.submit(new PeopleQueueTask(distinctId, "profile_set_once", properties));
  }

  /**
   * 为用户的一个或多个属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受{@link Number}
   * 类型
   *
   * @param distinctId    用户ID
   * @param properties    用户的属性
   */
  public void profileIncrement(String distinctId, Map<String, Object> properties)
      throws SensorsDataException {
    checkKey(distinctId);
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      if (!(prop.getValue() instanceof Number)) {
        throw new SensorsDataException("The value type in properties should be a numerical type.");
      }
    }
    this.executor.submit(new PeopleQueueTask(distinctId, "profile_increment", properties));
  }

  /**
   * 为用户的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   * @param value         属性的值
   */
  public void profileIncrement(String distinctId, String property, long value)
      throws SensorsDataException {
    checkKey(distinctId);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    this.executor.submit(new PeopleQueueTask(distinctId, "profile_increment", properties));
  }

  /**
   * 删除用户某一个属性
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   */
  public void profileUnset(String distinctId, String property) throws SensorsDataException {
    checkKey(distinctId);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, true);
    this.executor.submit(new PeopleQueueTask(distinctId, "profile_unset", properties));
  }

  /**
   * 删除一个用户的所有属性
   *
   * @param distinctId    用户ID
   */
  public void delete(String distinctId) throws SensorsDataException {
    checkKey(distinctId);
    executor.submit(new PeopleQueueTask(distinctId, "profile_unset", null));
  }

  /**
   * 立即发送缓存中的所有日志
   */
  public void flush() {
    this.lastFlushTime = System.currentTimeMillis();

    String dataEncoded = encodeAndClearTaskObjectList();
    try {
      if (!executor.isShutdown()) {
        executor.submit(new SubmitTask(dataEncoded));
      }
    } catch (RejectedExecutionException e) {
      log.warn(e.getMessage(), e);
    }
  }

  /**
   * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
   */
  public void shutdown() {
    flush();

    timerExecutor.shutdown();

    try {
      timerExecutor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }

    executor.shutdown();

    try {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  private class LowPriorityThreadFactory implements ThreadFactory {

    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setPriority(Thread.MIN_PRIORITY);
      return t;
    }

  }


  private class FlushTask implements Runnable {

    public FlushTask() {
    }

    public void run() {
      long waitTime = lastFlushTime + flushInterval - System.currentTimeMillis();
      while (waitTime > 0) {
        try {
          Thread.sleep(waitTime);
        } catch (InterruptedException e) {
          log.error(e.getMessage(), e);
        }
        waitTime = lastFlushTime + flushInterval - System.currentTimeMillis();
      }

      flush();

      try {
        if (!timerExecutor.isShutdown()) {
          timerExecutor.submit(new FlushTask());
        }
      } catch (RejectedExecutionException e) {
        log.warn(e.getMessage(), e);
      }
    }

  }


  private class SubmitTask implements Runnable {

    private final String dataEncoded;

    public SubmitTask(String dataEncoded) {
      this.dataEncoded = dataEncoded;
    }

    public void run() {
      if (this.dataEncoded == null) {
        return;
      }

      HttpClient httpclient = new DefaultHttpClient();
      HttpPost httpPost = new HttpPost(eventsEndPoint);

      try {
        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(1);

        // TODO
        // Zip the content
        //                if (config.getSendingGZipContent() == "true") {
        //                    nameValuePairs.add(new BasicNameValuePair("gzip", "1"));
        //                } else {
        nameValuePairs.add(new BasicNameValuePair("gzip", "0"));
        //                }

        nameValuePairs.add(new BasicNameValuePair("data_list", this.dataEncoded));

        httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
        httpPost.addHeader("User-Agent", "SensorsAnalytics Java SDK");

        HttpResponse response = httpclient.execute(httpPost);
        if (response.getStatusLine().getStatusCode() != 200) {
          log.warn(String.format("Response error. [status=%d error='%s']",
              response.getStatusLine().getStatusCode(),
              response.getStatusLine().getReasonPhrase()));
          Thread.sleep(1000);
          executor.submit(new SubmitTask(this.dataEncoded));
        }
        EntityUtils.consume(response.getEntity());
      } catch (ClientProtocolException e) {
        log.error(e.getMessage(), e);
      } catch (IOException e) {
        log.error(e.getMessage(), e);
      } catch (InterruptedException e) {
        log.error(e.getMessage(), e);
      }
    }

    private String inputStreamToString(final InputStream stream) throws IOException {
      BufferedReader br = new BufferedReader(new InputStreamReader(stream));
      StringBuilder sb = new StringBuilder();
      String line;
      while ((line = br.readLine()) != null) {
        sb.append(line).append("\n");
      }
      br.close();
      return sb.toString();
    }
  }


  private class EventsQueueTask implements Runnable {

    private final Map<String, Object> dataObj = new HashMap<String, Object>();

    public EventsQueueTask(String distinctId, String originDistinceId, String eventName,
        Map<String, Object> properties) {
      if (eventName.equals("$SignUp")) {
        dataObj.put("type", "track_signup");
      } else {
        dataObj.put("type", "track");
      }

      dataObj.put("event", eventName);
      dataObj.put("time", System.currentTimeMillis());

      dataObj.put("distinct_id", distinctId);
      if (originDistinceId != null) {
        dataObj.put("original_id", originDistinceId);
      }

      Map<String, Object> propertiesObj = new HashMap<String, Object>();

      for (Iterator<Map.Entry<String, Object>> iter = superProperties.entrySet().iterator(); iter
          .hasNext(); ) {
        Map.Entry<String, Object> item = iter.next();
        propertiesObj.put(item.getKey(), item.getValue());
      }

      if (properties != null) {
        for (Iterator<Map.Entry<String, Object>> iter = properties.entrySet().iterator(); iter
            .hasNext(); ) {
          Map.Entry<String, Object> item = iter.next();

          if (item.getKey().equals("$time")) {
            dataObj.put("time", ((Date) item.getValue()).getTime());
            continue;
          }

          propertiesObj.put(item.getKey(), item.getValue());
        }
      }

      dataObj.put("properties", propertiesObj);
    }

    public void run() {
      addToTaskObjectList(dataObj);
    }
  }


  private class PeopleQueueTask implements Runnable {

    private final Map<String, Object> dataObj = new HashMap<String, Object>();

    public PeopleQueueTask(String distinctId, String actionType, Map<String, Object> properties) {
      dataObj.put("type", actionType);
      dataObj.put("distinct_id", distinctId);

      dataObj.put("time", System.currentTimeMillis());

      Map<String, Object> propertiesObj = new HashMap<String, Object>();

      if (properties != null) {
        for (Iterator<Map.Entry<String, Object>> iter = properties.entrySet().iterator(); iter
            .hasNext(); ) {
          Map.Entry<String, Object> item = iter.next();

          if (item.getKey().equals("$time")) {
            dataObj.put("time", ((Date) item.getValue()).getTime());
            continue;
          }

          propertiesObj.put(item.getKey(), item.getValue());
        }
      }

      dataObj.put("properties", propertiesObj);
    }

    public void run() {
      addToTaskObjectList(dataObj);
    }
  }


  private void addToTaskObjectList(Map<String, Object> taskObject) {
    synchronized (this.taskObjectList) {
      this.taskObjectList.add(taskObject);
      if (this.taskObjectList.size() > bulkUploadLimit) {
        flush();
      }
    }
  }

  private String encodeAndClearTaskObjectList() {
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    synchronized (this.taskObjectList) {
      if (this.taskObjectList.size() == 0) {
        return null;
      }
      for (Map<String, Object> task : this.taskObjectList) {
        data.add(task);
      }
      this.taskObjectList.clear();
    }

    try {
      String dataJson = this.jsonObjectMapper.writeValueAsString(data);
      if (this.debugMode) {
        log.debug("Sent new log: " + dataJson);
      }
      return Base64Coder.encodeString(dataJson);
    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  private void checkKey(String key) throws SensorsDataException {
    if (key == null || key.length() < 1) {
      throw new SensorsDataException("The key is empty.");
    }

    if (!(KEY_PATTERN.matcher(key).matches())) {
      throw new SensorsDataException("The key '" + key + "' is invalid.");
    }
  }

  private void checkValue(String key, Object value) throws SensorsDataException {
    if (!(value instanceof Number)&& !(value instanceof Date)
        && !(value instanceof String) && !(value instanceof List<?>)) {
      throw new SensorsDataException("The value type in properties should be a basic type: "
          + "Number, String, Date, List<?>.");
    }

    if (key.equals("$time") && !(value instanceof Date)) {
      throw new SensorsDataException("The value type of '$time' in properties should be a "
          + "java.util.Date type.");
    }
  }

  private void checkTypeInProperties(Map<String, Object> properties) throws SensorsDataException {
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      checkKey(prop.getKey());
      checkValue(prop.getKey(), prop.getValue());
    }
  }
}
