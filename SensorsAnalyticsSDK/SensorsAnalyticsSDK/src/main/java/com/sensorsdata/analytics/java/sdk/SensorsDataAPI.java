package com.sensorsdata.analytics.java.sdk;

import com.sensorsdata.analytics.java.sdk.exceptions.ConnectErrorException;
import com.sensorsdata.analytics.java.sdk.exceptions.FlushErrorException;
import com.sensorsdata.analytics.java.sdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.java.sdk.exceptions.QueueLimitExceededException;
import com.sensorsdata.analytics.java.sdk.util.Base64Coder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * SensorsData Analytics API
 * 提供各种日志记录、用户属性记录的功能
 */
public class SensorsDataAPI {

  private final static Logger log = LoggerFactory.getLogger(SensorsDataAPI.class);
  private final static int EXECUTE_THREAD_NUMBER = 1;
  private final static int EXECUTE_THREAD_SUBMMIT_TTL = 3 * 1000;
  private final static int EXECUTE_QUEUE_SIZE = 32768;

  private final static Pattern KEY_PATTERN = Pattern.compile(
      "^((?!^distinct_id$|^original_id$|^time$|^properties$|^id$|^first_id$|^second_id$|^users$|^events$|^event$|^user_id$|^date$|^datetime$)[a-zA-Z_$][a-zA-Z\\d_$]{0,99})$",
      Pattern.CASE_INSENSITIVE);
  private final static SimpleDateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyy-mm-dd hh:mm:ss.SSS");

  private static SensorsDataAPI instance = null;

  private final String eventsEndPoint;
  private final int flushInterval;
  private final int bulkUploadLimit;
  private final boolean sendingGZipContent;
  private final boolean debugMode;

  private final ThreadPoolExecutor executor;
  private final ThreadPoolExecutor timerExecutor;

  private final Map<String, Object> superProperties;

  private final List<Map<String, Object>> taskObjectList;

  private ObjectMapper jsonObjectMapper;

  private long lastFlushTime;

  /**
   * 返回已经初始化的SensorsDataAPI单例
   *
   * 调用之前必须先调用 {@link #sharedInstanceWithServerURL(String)} 或 {@link #sharedInstanceWithConfigure(String,
   * int, int, boolean)} 初始化单例
   *
   * @return SensorsDataAPI单例
   */
  public static SensorsDataAPI sharedInstance() {
    if (instance == null) {
      log.error("SensorsDataAPI must be initialized before calling sharedInstance.");
    }
    return instance;
  }

  /**
   * 根据传入的所部署的SensorsAnalytics服务器的URL，返回一个SensorsDataAPI的单例。默认刷新周期为60秒或缓存1000条数据，默认关闭Debug
   * 模式。更多说明请参考 {@link #sharedInstanceWithConfigure(String, int, int, boolean)}
   *
   * @param serverUrl 接收日志的SensorsAnalytics服务器URL
   * @return SensorsDataAPI单例
   */
  public static SensorsDataAPI sharedInstanceWithServerURL(String serverUrl) {
    return sharedInstanceWithConfigure(serverUrl, 60 * 1000, 1000, false);
  }

  /**
   * 根据传入的配置，返回一个SensorsAnalyticsSDK的单例。
   * <p>
   * flushInterval是指两次发送的时间间隔，bulkSize是指本地日志缓存队列长度的发送阈值。在每次调用{@link #track(String, String)
   * }、{@link #trackSignUp(String, String)}、{@link #profileSet(String, java.util.Map)} 等方法时，都会检查如下条件，以判断是否向服务器上传数据:
   * 1. 当前是否是WIFI/3G/4G网络条件
   * 2. 与上次发送的时间间隔是否大于 flushInterval 或 缓存队列长度是否大于 bulkSize
   * 如果满足这两个条件，则向服务器发送一次数据；如果不满足，则把数据加入到队列中，等待下次发送。需要注意的是，为了避免占用过多内存，当频繁记录日志时，队列最多只缓存32768条数据。
   *
   * @param serverUrl     接收日志的SensorsAnalytics服务器URL
   * @param flushInterval 日志发送的时间间隔，单位毫秒
   * @param bulkSize      日志缓存的最大值，超过此值将立即进行发送
   * @param debugMode     Debug模式，该模式下会输出调试日志
   */
  public static SensorsDataAPI sharedInstanceWithConfigure(
      String serverUrl,
      int flushInterval,
      int bulkSize,
      boolean debugMode) {
    if (instance == null) {
      instance = new SensorsDataAPI(serverUrl, flushInterval, bulkSize, debugMode);
    } else {
      log.warn("SensorsDataAPI has been initialized. Ignore the configures.");
    }
    return instance;
  }

  SensorsDataAPI(
      String serverUrl,
      int flushInterval,
      int bulkSize,
      boolean debugMode) {
    this.eventsEndPoint = serverUrl;
    this.flushInterval = flushInterval;
    this.bulkUploadLimit = bulkSize;
    this.sendingGZipContent = true;
    this.debugMode = debugMode;

    this.superProperties = new HashMap<String, Object>();
    clearSuperProperties();

    this.taskObjectList = new ArrayList<Map<String, Object>>();

    this.lastFlushTime = System.currentTimeMillis();

    this.executor = new ThreadPoolExecutor(EXECUTE_THREAD_NUMBER, EXECUTE_THREAD_NUMBER,
        EXECUTE_THREAD_SUBMMIT_TTL, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<Runnable>
        (1), new LowPriorityThreadFactory());

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
    this.jsonObjectMapper.setDateFormat(DATE_FORMAT);
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
    this.superProperties.clear();
    this.superProperties.put("$lib", "java");
  }

  /**
   * 记录一个没有属性的事件
   *
   * @param distinctId  用户ID
   * @param eventName   事件名称
   */
  public void track(String distinctId, String eventName)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkKey(eventName);
    addEvent(distinctId, null, eventName, null);
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
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkKey(eventName);
    checkTypeInProperties(properties);
    addEvent(distinctId, null, eventName, properties);
  }

  /**
   * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:http://www.sensorsdata.cn/manual/track_signup.html，并在必要时联系我们的技术支持人员。
   *
   * @param distinctId       新的用户ID
   * @param originDistinctId 旧的用户ID
   */
  public void trackSignUp(String distinctId, String originDistinctId)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkDistinctId(originDistinctId);
    addEvent(distinctId, originDistinctId, "$SignUp", null);
  }

  /**
   * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:http://www.sensorsdata.cn/manual/track_signup.html，并在必要时联系我们的技术支持人员。
   * {@link Number}, {@link String}, {@link Date}和{@linkList<?>}，若属性包含 $time 字段，则
   * 它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * @param distinctId       新的用户ID
   * @param originDistinctId 旧的用户ID
   * @param properties       事件的属性
   */
  public void trackSignUp(String distinctId, String originDistinctId,
      Map<String, Object> properties) throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkDistinctId(originDistinctId);
    checkTypeInProperties(properties);
    addEvent(distinctId, originDistinctId, "$SignUp", properties);
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
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkTypeInProperties(properties);
    addProfile(distinctId, "profile_set", properties);
  }

  /**
   * 设置用户的属性。这个接口只能设置单个key对应的内容，同样，如果已经存在，则覆盖，否则，新创建
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   * @param value         属性的值
   */
  public void profileSet(String distinctId, String property, Object value)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    checkTypeInProperties(properties);
    addProfile(distinctId, "profile_set", properties);
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
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkTypeInProperties(properties);
    addProfile(distinctId, "profile_set_once", properties);
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
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    checkTypeInProperties(properties);
    addProfile(distinctId, "profile_set_once", properties);
  }

  /**
   * 为用户的一个或多个属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受{@link Number}
   * 类型
   *
   * @param distinctId    用户ID
   * @param properties    用户的属性
   */
  public void profileIncrement(String distinctId, Map<String, Object> properties)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      checkKey(prop.getKey());
      if (!(prop.getValue() instanceof Number)) {
        throw new InvalidArgumentException(
            "The value type in properties should be a numerical type.");
      }
    }
    addProfile(distinctId, "profile_increment", properties);
  }

  /**
   * 为用户的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   * @param value         属性的值
   */
  public void profileIncrement(String distinctId, String property, long value)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    checkKey(property);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    addProfile(distinctId, "profile_increment", properties);
  }

  /**
   * 删除用户某一个属性
   *
   * @param distinctId    用户ID
   * @param property      属性名称
   */
  public void profileUnset(String distinctId, String property)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, true);
    checkTypeInProperties(properties);
    addProfile(distinctId, "profile_unset", properties);
  }

  /**
   * 删除一个用户的所有属性
   *
   * @param distinctId    用户ID
   */
  public void delete(String distinctId)
      throws InvalidArgumentException, QueueLimitExceededException {
    checkDistinctId(distinctId);
    addProfile(distinctId, "profile_unset", null);
  }

  /**
   * 立即发送缓存中的所有日志
   */
  public void flush() throws ConnectErrorException, FlushErrorException {
    _flush(0);
  }

  /**
   * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
   */
  public void shutdown() throws ConnectErrorException, FlushErrorException {
    _flush(0);

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

  private void addEvent(String distinctId, String originDistinceId, String eventName,
      Map<String, Object> properties) throws QueueLimitExceededException {
    Map<String, Object> dataObj = new HashMap<String, Object>();

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

    for (Map.Entry<String, Object> item : superProperties.entrySet()) {
      propertiesObj.put(item.getKey(), item.getValue());
    }

    if (properties != null) {
      for (Map.Entry<String, Object> item : properties.entrySet()) {
        if (item.getKey().equals("$time")) {
          dataObj.put("time", ((Date) item.getValue()).getTime());
          continue;
        }

        propertiesObj.put(item.getKey(), item.getValue());
      }
    }

    dataObj.put("properties", propertiesObj);
    addToTaskObjectList(dataObj);
  }

  private void addProfile(String distinctId, String actionType, Map<String, Object> properties)
      throws QueueLimitExceededException {
    Map<String, Object> dataObj = new HashMap<String, Object>();
    dataObj.put("type", actionType);
    dataObj.put("distinct_id", distinctId);

    dataObj.put("time", System.currentTimeMillis());

    Map<String, Object> propertiesObj = new HashMap<String, Object>();

    if (properties != null) {
      for (Map.Entry<String, Object> item : properties.entrySet()) {
        if (item.getKey().equals("$time")) {
          dataObj.put("time", ((Date) item.getValue()).getTime());
          continue;
        }

        propertiesObj.put(item.getKey(), item.getValue());
      }
    }

    dataObj.put("properties", propertiesObj);
    addToTaskObjectList(dataObj);
  }

  private void addToTaskObjectList(Map<String, Object> taskObject) throws QueueLimitExceededException {
    synchronized (this.taskObjectList) {
      this.taskObjectList.add(taskObject);
      if (this.taskObjectList.size() > EXECUTE_QUEUE_SIZE) {
        throw new QueueLimitExceededException("The maximum length of sending queue is " +
            EXECUTE_QUEUE_SIZE);
      }
      if (this.taskObjectList.size() > bulkUploadLimit) {
        try {
          executor.submit(new SubmitTask(bulkUploadLimit));
        } catch (RejectedExecutionException e) {
          // do nothing
        }
      }
    }
  }

  private void _flush(int uploadLimit) throws FlushErrorException, ConnectErrorException {
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();
    synchronized (this.taskObjectList) {
      if (this.taskObjectList.isEmpty() || (this.taskObjectList.size() < uploadLimit &&
          uploadLimit > 0)) {
        return;
      }
      for (Map<String, Object> task : this.taskObjectList) {
        data.add(task);
      }
      this.taskObjectList.clear();
    }

    char[] dataEncoded = encodeTasks(data);
    if (dataEncoded == null) {
      return;
    }

    CloseableHttpClient httpClient = HttpClients.createDefault();
    HttpPost httpPost = new HttpPost(eventsEndPoint);

    try {
      List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>(1);

      if (sendingGZipContent) {
        nameValuePairs.add(new BasicNameValuePair("gzip", "1"));
      } else {
        nameValuePairs.add(new BasicNameValuePair("gzip", "0"));
      }

      nameValuePairs.add(new BasicNameValuePair("data_list", new String(dataEncoded)));

      httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
      httpPost.addHeader("User-Agent", "SensorsAnalytics Java SDK");

      HttpResponse response = httpClient.execute(httpPost);
      if (response.getStatusLine().getStatusCode() != 200) {
        throw new FlushErrorException(String.format("Response error. [status=%d error='%s']",
            response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase()));
      }
      EntityUtils.consume(response.getEntity());
    } catch (ClientProtocolException e) {
      throw new ConnectErrorException(e);
    } catch (IOException e) {
      throw new ConnectErrorException(e);
    }

    this.lastFlushTime = System.currentTimeMillis();
  }

  private char[] encodeTasks(List<Map<String, Object>> data) {
    try {
      byte[] dataJson = this.jsonObjectMapper.writeValueAsBytes(data);
      if (this.debugMode) {
        log.debug("Sent new log: " + this.jsonObjectMapper.writeValueAsString(data));
      }
      if (sendingGZipContent) {
        ByteArrayOutputStream os = new ByteArrayOutputStream(dataJson.length);
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(dataJson);
        gos.close();
        byte[] compressed = os.toByteArray();
        os.close();
        return Base64Coder.encode(compressed);
      } else {
        return Base64Coder.encode(dataJson);
      }

    } catch (JsonProcessingException e) {
      log.error(e.getMessage(), e);
    } catch (IOException e) {
      log.error(e.getMessage(), e);
    }
    return null;
  }

  private void checkDistinctId(String key) throws InvalidArgumentException {
    if (key == null || key.length() < 1) {
      throw new InvalidArgumentException("The distinct_id or original_id is empty.");
    }
    if (key.length() > 255) {
      throw new InvalidArgumentException("The max_length of distinct_id or original_id is 255.");
    }
  }

  private void checkKey(String key) throws InvalidArgumentException {
    if (key == null || key.length() < 1) {
      throw new InvalidArgumentException("The key is empty.");
    }
    if (!(KEY_PATTERN.matcher(key).matches())) {
      throw new InvalidArgumentException("The key '" + key + "' is invalid.");
    }
  }

  private void checkValue(String key, Object value) throws InvalidArgumentException {
    if (!(value instanceof Number) && !(value instanceof Date) && !(value instanceof String) &&
        !(value instanceof Boolean) && !(value instanceof List<?>)) {
      throw new InvalidArgumentException("The value type in properties should be a basic type: "
          + "Number, String, Date, Boolean, List<?>.");
    }

    if (key.equals("$time") && !(value instanceof Date)) {
      throw new InvalidArgumentException(
          "The value type of '$time' in properties should be a " + "java.util.Date type.");
    }
  }

  private void checkTypeInProperties(Map<String, Object> properties)
      throws InvalidArgumentException {
    for (Map.Entry<String, Object> prop : properties.entrySet()) {
      checkKey(prop.getKey());
      checkValue(prop.getKey(), prop.getValue());
    }
  }

  private class LowPriorityThreadFactory implements ThreadFactory {

    public Thread newThread(Runnable r) {
      Thread t = new Thread(r);
      t.setPriority(Thread.MIN_PRIORITY);
      return t;
    }

  }

  private class SubmitTask implements Runnable {
    public SubmitTask(int uploadLimit) {
      this.uploadLimit = uploadLimit;
    }

    public void run() {
      try {
        _flush(this.uploadLimit);
      } catch (FlushErrorException e) {
        log.error(e.getMessage(), e);
      } catch (ConnectErrorException e) {
        log.error(e.getMessage(), e);
      }
    }

    private final int uploadLimit;
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

      try {
        executor.submit(new SubmitTask(0));
      } catch (RejectedExecutionException e) {
        // do nothing
      }

      try {
        if (!timerExecutor.isShutdown()) {
          timerExecutor.submit(new FlushTask());
        }
      } catch (RejectedExecutionException e) {
        log.warn(e.getMessage(), e);
      }
    }
  }
}
