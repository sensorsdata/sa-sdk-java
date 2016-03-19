package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.exceptions.ConnectErrorException;
import com.sensorsdata.analytics.javasdk.exceptions.DebugModeException;
import com.sensorsdata.analytics.javasdk.exceptions.FlushErrorException;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.Base64Coder;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * Sensors Analytics SDK
 */
public class SensorsDataAPI {

  private final static Logger log = LoggerFactory.getLogger(SensorsDataAPI.class);

  private final static int EXECUTE_THREAD_NUMBER = 10;
  private final static String SDK_VERSION = "1.3.4";

  private final static Pattern KEY_PATTERN = Pattern.compile(
      "^((?!^distinct_id$|^original_id$|^time$|^properties$|^id$|^first_id$|^second_id$|^users$|^events$|^event$|^user_id$|^date$|^datetime$)[a-zA-Z_$][a-zA-Z\\d_$]{0,99})$",
      Pattern.CASE_INSENSITIVE);
  private final static SimpleDateFormat DATE_FORMAT =
      new SimpleDateFormat("yyyy-MM-dd hh:mm:ss.SSS");

  private static SensorsDataAPI instance = null;

  private final String eventsEndPoint;
  private final int flushInterval;
  private final int bulkUploadLimit;
  private final boolean sendingGZipContent;
  private final DebugMode debugMode;

  private final ThreadPoolExecutor executor;

  private final Map<String, Object> superProperties;

  private final List<Map<String, Object>> taskObjectList;

  private ObjectMapper jsonObjectMapper;


  /**
   * Debug模式，用于检验数据导入是否正确。该模式下，事件会逐条实时发送到SensorsAnalytics，并根据返回值检查
   * 数据导入是否正确。
   *
   * Debug模式的具体使用方式，请参考:
   *   http://www.sensorsdata.cn/manual/debug_mode.html
   *
   * Debug模式有三种：
   *   NONE - 关闭DEBUG模式
   *   DEBUG_ONLY - 打开DEBUG模式，但该模式下发送的数据仅用于调试，不进行数据导入
   *   DEBUG_AND_TRACK - 打开DEBUG模式，并将数据导入到SensorsAnalytics中
   */
  public enum DebugMode {
    DEBUG_OFF(false, false),
    DEBUG_ONLY(true, false),
    DEBUG_AND_TRACK(true, true);

    private final boolean mDebugMode;
    private final boolean mDebugWriteData;

    DebugMode(boolean debugMode, boolean debugWriteData) {
      mDebugMode = debugMode;
      mDebugWriteData = debugWriteData;
    }

    boolean isDebugMode() {
      return mDebugMode;
    }

    boolean isDebugWriteData() {
      return mDebugWriteData;
    }
  }

  /**
   * 返回已经初始化的 SensorsDataAPI 单例
   *
   * 调用之前必须先调用 {@link #sharedInstanceWithServerURL(String,
   * com.sensorsdata.analytics.javasdk.SensorsDataAPI.DebugMode)} 或 {@link
   * #sharedInstanceWithConfigure(String, int, int,
   * com.sensorsdata.analytics.javasdk.SensorsDataAPI.DebugMode)}
   * 进行初始化
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
   * 初始化 Sensors Analytics SDK，并返回 SensorsDataAPI 的单例
   *
   * 默认刷新周期为1秒或缓存1000条数据。更多说明请参考
   * {@link #sharedInstanceWithConfigure(String, int, int,
   * com.sensorsdata.analytics.javasdk.SensorsDataAPI.DebugMode)}
   *
   * @param serverUrl Sensors Analytics 采集数据的 Url
   * @param debugMode Debug模式选项，参考
   *                  {@link com.sensorsdata.analytics.javasdk.SensorsDataAPI.DebugMode}
   *
   * @return SensorsDataAPI单例
   */
  public static SensorsDataAPI sharedInstanceWithServerURL(final String serverUrl, final
  DebugMode debugMode) {
    return sharedInstanceWithConfigure(serverUrl, 1000, 1000, debugMode);
  }

  /**
   * 初始化 Sensors Analytics SDK，并返回 SensorsDataAPI 的单例
   *
   * flushInterval是指两次发送的时间间隔，bulkSize是发送缓存日志条目数的阈值。
   *
   * 在每次调用{@link #track(String, String)}、{@link #trackSignUp(String, String)}、
   * {@link #profileSet(String, java.util.Map)} 等方法时，都会检查如下条件，以判断是否向服务器上传数据:
   *
   *   1. 上次发送的时间间隔是否大于 flushInterval
   *   2. 缓存队列长度是否大于 bulkSize
   *
   * 如果满足这两个条件之一，则向服务器发送一次数据；如果不满足，则把数据加入到队列中，等待下次发送。
   *
   * 需要注意的是，当频繁记录事件时，需要小心缓存队列占用过多内存。此时可以通过 {@link #flush()} 强制触发
   * 日志发送
   *
   * @param serverUrl     Sensors Analytics 采集数据的 Url
   * @param flushInterval 日志发送的时间间隔，单位毫秒
   * @param bulkSize      日志缓存的最大值，超过此值将立即进行发送
   * @param debugMode     Debug模式选项，参考
   *                      {@link com.sensorsdata.analytics.javasdk.SensorsDataAPI.DebugMode}
   *
   * @return SensorsDataAPI实例
   */
  public static SensorsDataAPI sharedInstanceWithConfigure(final String serverUrl,
      final int flushInterval, final int bulkSize, final DebugMode debugMode) {
    if (instance == null) {
      String url = serverUrl;
      if (debugMode.isDebugMode()) {
        try {
          // 将 URI Path 替换成 Debug 模式的 '/debug'
          URIBuilder builder = new URIBuilder(new URI(serverUrl));
          builder.setPath("/debug");
          url = builder.build().toURL().toString();
        } catch (URISyntaxException e) {
          throw new DebugModeException("Invalid server url of Sensors Analytics.");
        } catch (MalformedURLException e) {
          throw new DebugModeException("Invalid server url of Sensors Analytics.");
        }
      }
      instance = new SensorsDataAPI(url, flushInterval, bulkSize, debugMode);
    } else {
      log.warn("SensorsDataAPI has been initialized. Ignore the configures.");
    }
    return instance;
  }

  SensorsDataAPI(final String serverUrl, final int flushInterval, final int bulkSize,
      final DebugMode debugMode) {
    this.eventsEndPoint = serverUrl;
    this.flushInterval = flushInterval;
    this.bulkUploadLimit = bulkSize;
    this.sendingGZipContent = true;
    this.debugMode = debugMode;

    this.superProperties = new HashMap<String, Object>();
    clearSuperProperties();

    this.taskObjectList = new ArrayList<Map<String, Object>>();

    this.executor = new ThreadPoolExecutor(EXECUTE_THREAD_NUMBER, EXECUTE_THREAD_NUMBER, 0L,
        TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(), new ThreadFactory() {
      public Thread newThread(Runnable r) {
        Thread t = new Thread(r);
        t.setPriority(Thread.MIN_PRIORITY);
        return t;
      }
    });
    this.executor.execute(new FlushTask());

    this.jsonObjectMapper = new ObjectMapper();
    // 容忍json中出现未知的列
    this.jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // 兼容java中的驼峰的字段名命名
    this.jsonObjectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    this.jsonObjectMapper.setDateFormat(DATE_FORMAT);
  }

  /**
   * 设置每个事件都带有的一些公共属性
   *
   * 当track的Properties，superProperties和SDK自动生成的automaticProperties有相同的key时，遵循如下的优先级：
   *    track.properties 高于 superProperties 高于 automaticProperties
   *
   * 另外，当这个接口被多次调用时，是用新传入的数据去merge先前的数据
   *
   * 例如，在调用接口前，dict是 {"a":1, "b": "bbb"}，传入的dict是 {"b": 123, "c": "asd"}，则merge后
   * 的结果是 {"a":1, "b": 123, "c": "asd"}
   *
   * @param superPropertiesMap 一个或多个公共属性
   */
  public void registerSuperProperties(Map<String, Object> superPropertiesMap) {
    for (Map.Entry<String, Object> item : superPropertiesMap.entrySet()) {
      this.superProperties.put(item.getKey(), item.getValue());
    }
  }

  /**
   * 清除公共属性
   */
  public void clearSuperProperties() {
    this.superProperties.clear();
    this.superProperties.put("$lib", "Java");
    this.superProperties.put("$lib_version", SDK_VERSION);
  }

  /**
   * 记录一个没有任何属性的事件
   *
   * @param distinctId 用户ID
   * @param eventName  事件名称
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> track(String distinctId, String eventName)
      throws InvalidArgumentException {
    return track(distinctId, eventName, null);
  }

  /**
   * 记录一个拥有一个或多个属性的事件。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和
   * {@link List}，若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * @param distinctId 用户ID
   * @param eventName  事件名称
   * @param properties 事件的属性
   *
   * @return {@link java.util.concurrent.Future}对象。本方法会异步执行，正常情况下不需要关心该返回对象。
   * 若需要等待track及flush同步完成，可调用该对象的 <code>.get()</code> 方法，它会阻塞直到track及flush
   * 执行结束。执行过程中任何异常都会触发 {@link ExecutionException}。
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> track(String distinctId, String eventName, Map<String, Object> properties)
      throws InvalidArgumentException {
    return addEvent(distinctId, distinctId, "track", eventName, properties);
  }

  /**
   * 记录用户注册事件
   *
   * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
   * http://www.sensorsdata.cn/manual/track_signup.html
   * 并在必要时联系我们的技术支持人员。
   *
   * @param distinctId       新的用户ID
   * @param originDistinctId 旧的用户ID
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> trackSignUp(String distinctId, String originDistinctId)
      throws InvalidArgumentException {
    return trackSignUp(distinctId, originDistinctId, null);
  }

  /**
   * 记录用户注册事件
   *
   * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
   * http://www.sensorsdata.cn/manual/track_signup.html
   * 并在必要时联系我们的技术支持人员。
   * <p>
   * 属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@link List}，若属性包
   * 含 $time 字段，它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * @param distinctId       新的用户ID
   * @param originDistinctId 旧的用户ID
   * @param properties       事件的属性
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> trackSignUp(String distinctId, String originDistinctId,
      Map<String, Object> properties) throws InvalidArgumentException {
    return addEvent(distinctId, originDistinctId, "track_signup", "$SignUp", properties);
  }

  /**
   * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@link List}，
   * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
   *
   * @param distinctId 用户ID
   * @param properties 用户的属性
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileSet(String distinctId, Map<String, Object> properties)
      throws InvalidArgumentException {
    return addEvent(distinctId, distinctId, "profile_set", null, properties);
  }

  /**
   * 设置用户的属性。这个接口只能设置单个key对应的内容，同样，如果已经存在，则覆盖，否则，新创建
   *
   * @param distinctId 用户ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileSet(String distinctId, String property, Object value)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    return profileSet(distinctId, properties);
  }

  /**
   * 首次设置用户的属性。
   * 属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@link List}，
   * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * 与profileSet接口不同的是：
   * 如果要设置的properties的key，在这个用户的profile中已经存在，则不处理，否则，新创建
   *
   * @param distinctId 用户ID
   * @param properties 用户的属性
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileSetOnce(String distinctId, Map<String, Object> properties)
      throws InvalidArgumentException {
    return addEvent(distinctId, distinctId, "profile_set_once", null, properties);
  }

  /**
   * 首次设置用户的属性。这个接口只能设置单个key对应的内容。
   * 与profileSet接口不同的是，如果key的内容之前已经存在，则不处理，否则，重新创建
   *
   * @param distinctId 用户ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileSetOnce(String distinctId, String property, Object value)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    return profileSetOnce(distinctId, properties);
  }

  /**
   * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受
   * {@link Number}类型
   *
   * @param distinctId 用户ID
   * @param properties 用户的属性
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileIncrement(String distinctId, Map<String, Object> properties)
      throws InvalidArgumentException {
    return addEvent(distinctId, distinctId, "profile_increment", null, properties);
  }

  /**
   * 为用户的数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
   *
   * @param distinctId 用户ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileIncrement(String distinctId, String property, long value)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    return profileIncrement(distinctId, properties);
  }

  /**
   * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型
   * 必须为 {@link java.lang.String}
   *
   * @param distinctId 用户ID
   * @param properties 用户的属性
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileAppend(String distinctId, Map<String, Object> properties)
      throws InvalidArgumentException {
    return addEvent(distinctId, distinctId, "profile_append", null, properties);
  }

  /**
   * 为用户的数组类型的属性追加一个字符串
   *
   * @param distinctId 用户ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileAppend(String distinctId, String property, String value)
      throws InvalidArgumentException {
    List<String> values = new ArrayList<String>();
    values.add(value);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, values);
    return profileAppend(distinctId, properties);
  }

  /**
   * 删除用户某一个属性
   *
   * @param distinctId 用户ID
   * @param property   属性名称
   *
   * @return {@link java.util.concurrent.Future} 对象，参考
   * {@link #track(String, String, java.util.Map)}
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public Future<Boolean> profileUnset(String distinctId, String property)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, true);
    return addEvent(distinctId, distinctId, "profile_unset", null, properties);
  }

  /**
   * 立即发送缓存中的所有日志，阻塞等待直到发送完成
   */
  public void flush() {
    Future<Boolean> task = enqueueAndFlush(null, 0);
    try {
      task.get();
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    } catch (ExecutionException e) {
      log.error(e.getMessage(), e);
    }
  }

  /**
   * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
   */
  public void shutdown() {
    flush();
    executor.shutdown();
    try {
      executor.awaitTermination(30, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      log.error(e.getMessage(), e);
    }
  }

  private Future<Boolean> addEvent(String distinctId, String originDistinceId, String actionType,
      String eventName, Map<String, Object> properties) throws InvalidArgumentException {
    assertKey("Distinct Id", distinctId);
    assertKey("Original Distinct Id", originDistinceId);
    if (actionType.equals("track")) {
      assertKey("Event Name", eventName);
    }
    assertProperties(actionType, properties);

    long time = extract_time_from_properties(properties);

    Map<String, Object> eventProperties = new HashMap<String, Object>();
    if (actionType.equals("track") || actionType.equals("track_signup")) {
      eventProperties.putAll(superProperties);
    }
    if (properties != null) {
      eventProperties.putAll(properties);
    }

    Map<String, Object> event = new HashMap<String, Object>();

    event.put("type", actionType);
    event.put("time", time);
    event.put("distinct_id", distinctId);
    event.put("properties", eventProperties);

    if (actionType.equals("track") || actionType.equals("track_signup")) {
      event.put("event", eventName);
    }
    if (actionType.equals("track_signup")) {
      event.put("original_id", originDistinceId);
    }

    if (this.debugMode.isDebugMode()) {
      Future<Boolean> task = enqueueAndFlush(event, 0);
      try {
        task.get();
      } catch (InterruptedException e) {
        throw new DebugModeException(e.getMessage());
      } catch (ExecutionException e) {
        if (e.getCause() instanceof FlushErrorException) {
          String error = String
              .format("Unexpected response from SensorsAnalytics server. [error='%s']",
                  e.getMessage());
          throw new DebugModeException(error);
        } else if (e.getCause() instanceof ConnectErrorException) {
          String error = String
              .format("Unexpected response from SensorsAnalytics server. [error='%s']",
                  e.getMessage());
          throw new DebugModeException(error);
        } else {
          throw new DebugModeException(e.getMessage());
        }
      }
      return task;
    } else {
      return enqueueAndFlush(event, this.bulkUploadLimit);
    }
  }

  private Future<Boolean> enqueueAndFlush(final Map<String, Object> event, final int flushBulk) {
    List<Map<String, Object>> data = new ArrayList<Map<String, Object>>();

    synchronized (this.taskObjectList) {
      if (null != event) {
        this.taskObjectList.add(event);
      }

      if (this.taskObjectList.isEmpty() || this.taskObjectList.size() < flushBulk) {
        // 返回空任务
        return executor.submit(new Callable<Boolean>() {
          @Override
          public Boolean call() throws Exception {
            return true;
          }
        });
      }

      data.addAll(this.taskObjectList);
      this.taskObjectList.clear();
    }
    return this.executor.submit(new SubmitTask(data));
  }

  private long extract_time_from_properties(Map<String, Object> properties) {
    if (properties == null || !properties.containsKey("$time")) {
      return System.currentTimeMillis();
    }
    Date time = (Date)properties.get("$time");
    properties.remove("$time");
    return time.getTime();
  }

  private void assertKey(String type, String key) throws InvalidArgumentException {
    if (key == null || key.length() < 1) {
      throw new InvalidArgumentException("The " + type + " is empty.");
    }
    if (key.length() > 255) {
      throw new InvalidArgumentException("The " + type + " is too long, max length is 255.");
    }
  }

  private void assertKeyWithRegex(String type, String key) throws InvalidArgumentException {
    assertKey(type, key);
    if (!(KEY_PATTERN.matcher(key).matches())) {
      throw new InvalidArgumentException("The " + type + "'" + key + "' is invalid.");
    }
  }

  private void assertProperties(String eventType, Map<String, Object> properties)
      throws InvalidArgumentException {
    if (null == properties) {
      return;
    }
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      assertKeyWithRegex("property", property.getKey());

      if (!(property.getValue() instanceof Number) && !(property.getValue() instanceof Date) && !
          (property.getValue() instanceof String) && !(property.getValue() instanceof Boolean) &&
          !(property.getValue() instanceof List<?>)) {
        throw new InvalidArgumentException("The property value should be a basic type: "
            + "Number, String, Date, Boolean, List<String>.");
      }

      if (property.getKey().equals("$time") && !(property.getValue() instanceof Date)) {
        throw new InvalidArgumentException(
            "The property value of key '$time' should be a java.util.Date type.");
      }

      // List 类型的属性值，List 元素必须为 String 类型
      if (property.getValue() instanceof List<?>) {
        for (Object v : (List<Object>) property.getValue()) {
          if (!(v instanceof String)) {
            throw new InvalidArgumentException("The property value should be a basic type: "
                + "Number, String, Date, Boolean, List<String>.");
          }
          if (((String) v).length() > 255) {
            throw new InvalidArgumentException("The property value is too long");
          }
        }
      }

      // String 类型的属性值，长度不能超过255
      if (property.getValue() instanceof String) {
        if (((String) property.getValue()).length() > 255) {
          throw new InvalidArgumentException("The property value is too long");
        }
      }

      if (eventType.equals("profile_increment")) {
        if (!(property.getValue() instanceof Number)) {
          throw new InvalidArgumentException("The property value of PROFILE_INCREMENT should be a "
              + "Number.");
        }
      } else if (eventType.equals("profile_append")) {
        if (!(property.getValue() instanceof List<?>)) {
          throw new InvalidArgumentException("The property value of PROFILE_INCREMENT should be a "
              + "List<String>.");
        }
      }
    }
  }

  private class SubmitTask implements Callable<Boolean> {

    public SubmitTask(final List<Map<String, Object>> data) {
      this.data = data;
    }

    @Override public Boolean call() throws FlushErrorException, ConnectErrorException {
      try {
        return sendData(this.data);
      } catch (FlushErrorException e) {
        log.error(e.getMessage(), e);
        System.out.println("Sensors Analytics SDK ERROR: " + e.getMessage());
        throw e;
      } catch (ConnectErrorException e) {
        log.error(e.getMessage(), e);
        System.out.println("Sensors Analytics SDK ERROR: " + e.getMessage());
        throw e;
      }
    }

    private Boolean sendData(final List<Map<String, Object>> data)
        throws FlushErrorException, ConnectErrorException {
      try {
        CloseableHttpClient httpClient = HttpClients.createDefault();
        HttpPost httpPost = new HttpPost(eventsEndPoint);

        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

        if (sendingGZipContent) {
          nameValuePairs.add(new BasicNameValuePair("gzip", "1"));
        } else {
          nameValuePairs.add(new BasicNameValuePair("gzip", "0"));
        }

        String dataJson = jsonObjectMapper.writeValueAsString(data);
        nameValuePairs.add(new BasicNameValuePair("data_list", new String(encodeTasks(dataJson))));

        httpPost.setEntity(new UrlEncodedFormEntity(nameValuePairs));
        httpPost.addHeader("User-Agent", "SensorsAnalytics Java SDK");
        if (debugMode.isDebugMode() && !debugMode.isDebugWriteData()) {
          httpPost.addHeader("Dry-Run", "true");
        }

        HttpResponse response = httpClient.execute(httpPost);
        int response_code = response.getStatusLine().getStatusCode();
        String response_body = EntityUtils.toString(response.getEntity(), "UTF-8");

        if (debugMode.isDebugMode()) {
          System.out.println
              ("==========================================================================");
          if (response_code == 200) {
            System.out.println(String.format("valid message: [%s]", dataJson));
          } else {
            System.out.println(String.format("invalid message: [%s]", dataJson));
            System.out.println(String.format("ret_code: %d", response_code));
            System.out.println(String.format("ret_content: %s", response_body));
          }
        }

        if (response_code != 200) {
          throw new FlushErrorException(String.format("Response error. [status=%d error='%s']",
              response.getStatusLine().getStatusCode(), response.getStatusLine().getReasonPhrase()));
        }

        return true;
      } catch (JsonGenerationException e) {
        throw new FlushErrorException(e);
      } catch (JsonMappingException e) {
        throw new FlushErrorException(e);
      } catch (ClientProtocolException e) {
        throw new ConnectErrorException(e);
      } catch (IOException e) {
        throw new ConnectErrorException(e);
      }
    }

    private char[] encodeTasks(String data) throws IOException {
      if (sendingGZipContent) {
        ByteArrayOutputStream os = new ByteArrayOutputStream(data.getBytes().length);
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(data.getBytes());
        gos.close();
        byte[] compressed = os.toByteArray();
        os.close();
        return Base64Coder.encode(compressed);
      } else {
        return Base64Coder.encode(data.getBytes());
      }
    }

    private final List<Map<String, Object>> data;
  }


  private class FlushTask implements Runnable {

    @Override public void run() {

      try {
        Thread.sleep(flushInterval);
      } catch (InterruptedException e) {
        log.warn(e.getMessage(), e);
      }

      if (executor.isShutdown()) {
        return;
      }

      flush();

      try {
        executor.submit(new FlushTask());
      } catch (RejectedExecutionException e) {
        // do nothing
      }
    }
  }
}
