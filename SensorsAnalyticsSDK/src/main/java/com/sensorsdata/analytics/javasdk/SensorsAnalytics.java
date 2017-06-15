package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.exceptions.DebugModeException;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.Base64Coder;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.spi.LoggingEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.Writer;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.charset.Charset;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;

/**
 * Sensors Analytics SDK
 */
public class SensorsAnalytics {

  private boolean enableTimeFree = false;

  public boolean isEnableTimeFree() {
    return enableTimeFree;
  }

  public void setEnableTimeFree(boolean enableTimeFree) {
    this.enableTimeFree = enableTimeFree;
  }

  private interface Consumer {
    void send(Map<String, Object> message);

    void flush();

    void close();
  }


  public static class DebugConsumer implements Consumer {

    public DebugConsumer(final String serverUrl, final boolean writeData) {
      String debugUrl = null;
      try {
        // 将 URI Path 替换成 Debug 模式的 '/debug'
        URIBuilder builder = new URIBuilder(new URI(serverUrl));
        String[] urlPathes = builder.getPath().split("/");
        urlPathes[urlPathes.length - 1] = "debug";
        builder.setPath(strJoin(urlPathes, "/"));
        debugUrl = builder.build().toURL().toString();
      } catch (URISyntaxException e) {
        throw new DebugModeException(e);
      } catch (MalformedURLException e) {
        throw new DebugModeException(e);
      }

      Map<String, String> headers = new HashMap<String, String>();
      if (!writeData) {
        headers.put("Dry-Run", "true");
      }

      this.httpConsumer = new HttpConsumer(debugUrl, headers);
      this.jsonMapper = getJsonObjectMapper();
    }

    @Override public void send(Map<String, Object> message) {
      // XXX: HttpConsumer 只处理了 Message List 的发送？
      List<Map<String, Object>> messageList = new ArrayList<Map<String, Object>>();
      messageList.add(message);

      String sendingData = null;
      try {
        sendingData = jsonMapper.writeValueAsString(messageList);
      } catch (JsonProcessingException e) {
        log.error("Failed to serialize data.", e);
        return;
      }

      System.out
          .println("==========================================================================");

      try {
        synchronized (httpConsumer) {
          httpConsumer.consume(sendingData);
        }

        System.out.println(String.format("valid message: %s", sendingData));
      } catch (IOException e) {
        log.error("Failed to send message with DebugConsumer.", e);
        throw new DebugModeException(e);
      } catch (HttpConsumer.HttpConsumerException e) {
        System.out.println(String.format("invalid message: %s", e.getSendingData()));
        System.out.println(String.format("http status code: %d", e.getHttpStatusCode()));
        System.out.println(String.format("http content: %s", e.getHttpContent()));
        throw new DebugModeException(e);
      }
    }

    @Override public void flush() {
      // do NOTHING
    }

    @Override public void close() {
      // do NOTHING;
    }

    final HttpConsumer httpConsumer;
    final ObjectMapper jsonMapper;
  }


  public static class BatchConsumer implements Consumer {

    private int flushCount = 0;

    public BatchConsumer(final String serverUrl) {
      this(serverUrl, MAX_FLUSH_BULK_SIZE);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize) {
      this(serverUrl, bulkSize, false);
    }

    public BatchConsumer(final String serverUrl, final int bulkSize, final boolean throwException) {
      this.messageList = new ArrayList<Map<String, Object>>();
      this.httpConsumer = new HttpConsumer(serverUrl, null);
      this.jsonMapper = getJsonObjectMapper();
      this.bulkSize = Math.min(MAX_FLUSH_BULK_SIZE, bulkSize);
      this.throwException = throwException;
    }

    @Override public void send(Map<String, Object> message) {
      synchronized (messageList) {
        messageList.add(message);
        if (messageList.size() >= bulkSize && System.currentTimeMillis() > retryTime) {
          flush();
        }
      }
    }

    @Override public void flush() {
      synchronized (messageList) {
        String sendingData = null;
        try {
          sendingData = jsonMapper.writeValueAsString(messageList);
        } catch (JsonProcessingException e) {
          log.error("Failed to serialize data.", e);
          messageList.clear();
        }

        try {
          this.httpConsumer.consume(sendingData);
          messageList.clear();
          reties = 0;
        } catch (IOException e) {
          log.error(String
              .format("Failed to dump message with BatchConsumer, retied %d times.", reties), e);
          if (throwException) {
            throw new RuntimeException(e);
          } else {
            // XXX: 发生错误时，默认0.1秒后才重试
            retryTime = System.currentTimeMillis() + 100;
            reties = reties + 1;
          }
        } catch (HttpConsumer.HttpConsumerException e) {
          log.error(String
              .format("Failed to dump message with BatchConsumer, retied %d times.", reties), e);
          if (throwException) {
            throw new RuntimeException(e);
          } else {
            // XXX: 发生错误时，默认0.1秒后才重试
            retryTime = System.currentTimeMillis() + 100;
            reties = reties + 1;
          }
        }
      }
    }

    @Override public void close() {
      flush();
    }

    private final static int MAX_FLUSH_BULK_SIZE = 50;

    private final List<Map<String, Object>> messageList;
    private final HttpConsumer httpConsumer;
    private final ObjectMapper jsonMapper;
    private final int bulkSize;
    private final boolean throwException;

    // Batch 发送重试时间
    private long retryTime = 0;

    // Retries
    private long reties = 0;
  }


  @Deprecated
  public interface AsyncBatchConsumerCallback {
    void onFlushTask(Future<Boolean> task);
  }


  /**
   * @deprecated Async模式下，开发者需要仔细处理缓存中的数据，如由于异步发送不及时导致缓存队列过大、程序停止时缓
   * 存队列清空等问题。因此我们建议开发者使用 LoggingConsumer 结合 LogAgent 工具导入数据。
   */
  @Deprecated
  public static class AsyncBatchConsumer implements Consumer {

    public AsyncBatchConsumer(final String serverUrl, final int bulkSize,
        final ThreadPoolExecutor executor, final AsyncBatchConsumerCallback callback) {
      this.messageList = new ArrayList<Map<String, Object>>();
      this.httpConsumer = new HttpConsumer(serverUrl, null);
      this.jsonMapper = getJsonObjectMapper();
      this.bulkSize = Math.min(MAX_FLUSH_BULK_SIZE, bulkSize);
      this.executor = executor;
      this.callback = callback;
    }

    @Override public void send(Map<String, Object> message) {
      synchronized (messageList) {
        messageList.add(message);
        if (messageList.size() >= bulkSize) {
          flush();
        }
      }
    }

    @Override public void flush() {
      synchronized (messageList) {
        try {
          final String sendingData = jsonMapper.writeValueAsString(messageList);
          final Future<Boolean> task = executor.submit(new Callable<Boolean>() {
            @Override public Boolean call() throws Exception {
              int reties = 0;
              while (reties < 5) {
                try {
                  httpConsumer.consume(sendingData);
                  break;
                } catch (IOException e) {
                  log.error(String.format(
                      "Failed to dump message with AsyncBatchConsumer, retied %d times:" + " "
                          + "%s", reties, sendingData), e);
                  // XXX: 发生错误时，默认1秒后才重试
                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e1) {
                  }
                  reties = reties + 1;
                } catch (HttpConsumer.HttpConsumerException e) {
                  log.error(String.format(
                      "Failed to dump message with AsyncBatchConsumer, retied %d times:" + " "
                          + "%s", reties, sendingData), e);
                  // XXX: 发生错误时，默认1秒后才重试
                  try {
                    Thread.sleep(1000);
                  } catch (InterruptedException e1) {
                  }
                  reties = reties + 1;
                }
              }

              if (reties >= 5) {
                return false;
              }
              return true;
            }
          });
          if (callback != null) {
            callback.onFlushTask(task);
          }
        } catch (JsonProcessingException e) {
          log.error("Failed to serialize data.", e);
        } finally {
          messageList.clear();
        }
      }
    }

    @Override public void close() {
      flush();

      executor.shutdown();
      try {
        executor.awaitTermination(10, TimeUnit.SECONDS);
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }

    private final static int MAX_FLUSH_BULK_SIZE = 50;

    private final List<Map<String, Object>> messageList;
    private final HttpConsumer httpConsumer;
    private final ObjectMapper jsonMapper;
    private final ThreadPoolExecutor executor;
    private final AsyncBatchConsumerCallback callback;
    private final int bulkSize;
  }


  public static class ConsoleConsumer implements Consumer {

    public ConsoleConsumer(final Writer writer) {
      this.jsonMapper = getJsonObjectMapper();
      this.writer = writer;
    }

    @Override public void send(Map<String, Object> message) {
      try {
        synchronized (writer) {
          writer.write(jsonMapper.writeValueAsString(message));
          writer.write("\n");
        }
      } catch (IOException e) {
        log.warn("Failed to dump message with ConsoleConsumer.", e);
      }
    }

    @Override public void flush() {
      // do NOTHING
    }

    @Override public void close() {
      // do NOTHING;
    }

    private final ObjectMapper jsonMapper;
    private final Writer writer;
  }


  public static class LoggingConsumer implements Consumer {
    final DailyRollingFileAppender dailyRollingFileAppender;
    private final ObjectMapper jsonMapper;
    org.apache.log4j.Logger logger;

    public LoggingConsumer(String filename) throws IOException {
      dailyRollingFileAppender =
          new DailyRollingFileAppender(new PatternLayout("%m"), filename, "'.'yyyy-MM-dd");
      dailyRollingFileAppender.setThreshold(Level.INFO);
      dailyRollingFileAppender.activateOptions();
      logger = org.apache.log4j.Logger.getLogger("SensorsAnalyticsLogger");
      this.jsonMapper = getJsonObjectMapper();
    }

    @Override public void send(Map<String, Object> message) {
      LoggingEvent loggingEvent = null;
      try {
        loggingEvent = new LoggingEvent(null, logger, System.currentTimeMillis(), Level.INFO,
            jsonMapper.writeValueAsString(message) + "\n", null);
      } catch (JsonProcessingException e) {
        log.warn("fail to process json", e);
        return;
      }
      synchronized (dailyRollingFileAppender) {
        dailyRollingFileAppender.append(loggingEvent);
      }
    }

    @Override public void flush() {
    }

    @Override public void close() {
      dailyRollingFileAppender.close();
    }
  }


  public static class ConcurrentLoggingConsumer implements Consumer {

    static class LoggingFileWriter {

      private final String fileName;
      private final FileOutputStream outputStream;

      LoggingFileWriter(final String fileName) throws FileNotFoundException {
        this.outputStream = new FileOutputStream(fileName, true);
        this.fileName = fileName;
      }

      void close() {
        try {
          outputStream.close();
          log.info("closed old file. [filename={}]", fileName);
        } catch (Exception e) {
          log.warn("fail to close output stream.", e);
        }
      }

      boolean isValid(final String fileName) {
        return this.fileName.equals(fileName);
      }

      boolean write(final StringBuilder sb) {
        boolean result = true;

        FileLock lock = null;
        try {
          final FileChannel channel = outputStream.getChannel();
          lock = channel.lock(0, Long.MAX_VALUE, false);
          outputStream.write(sb.toString().getBytes("UTF-8"));
        } catch (Exception e) {
          log.warn("fail to write file.", e);
          result = false;
        } finally {
          if (lock != null) {
            try {
              lock.release();
            } catch (IOException e) {
              log.warn("fail to release file lock.", e);
            }
          }
        }

        return result;
      }

    }

    private final static int BUFFER_LIMITATION = 1 * 1024 * 1024 * 1024;    // 1G

    private final ObjectMapper jsonMapper;
    private final String filenamePrefix;
    private final StringBuilder messageBuffer;
    private final int bufferSize;
    private final SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");

    private LoggingFileWriter fileWriter;

    public ConcurrentLoggingConsumer(String filenamePrefix) throws IOException {
      this(filenamePrefix, 8192);
    }

    public ConcurrentLoggingConsumer(String filenamePrefix, int bufferSize) throws IOException {
      this.filenamePrefix = filenamePrefix;
      this.jsonMapper = getJsonObjectMapper();
      this.messageBuffer = new StringBuilder(bufferSize);
      this.bufferSize = bufferSize;
    }

    @Override public synchronized void send(Map<String, Object> message) {
      if (messageBuffer.length() < BUFFER_LIMITATION) {
        try {
          messageBuffer.append(jsonMapper.writeValueAsString(message));
          messageBuffer.append("\n");
        } catch (JsonProcessingException e) {
          log.warn("fail to process json", e);
          return;
        }
      } else {
        log.error("logging buffer exceeded the allowed limitation.");
      }

      if (messageBuffer.length() >= bufferSize) {
        flush();
      }
    }

    private String constructFileName(Date now) {
      return filenamePrefix + "." + simpleDateFormat.format(now);
    }

    @Override public synchronized void flush() {
      if (messageBuffer.length() == 0) {
        return;
      }

      String filename = constructFileName(new Date());

      if (fileWriter != null && !fileWriter.isValid(filename)) {
        fileWriter.close();
        fileWriter = null;
      }

      if (fileWriter == null) {
        try {
          fileWriter = new LoggingFileWriter(filename);
          log.info("opened new file. [filename={}]", filename);
        } catch (FileNotFoundException e) {
          log.warn("failed to open file. [filename={}]", filename);
        }
      }

      if (fileWriter.write(messageBuffer)) {
        messageBuffer.setLength(0);
      }
    }

    @Override public synchronized void close() {
      flush();

      fileWriter.close();
      fileWriter = null;
    }
  }

  public SensorsAnalytics(final Consumer consumer) {
    this.consumer = consumer;

    this.superProperties = new ConcurrentHashMap<String, Object>();
    clearSuperProperties();
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
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param eventName  事件名称
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void track(String distinctId, boolean isLoginId, String eventName)
      throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "track", eventName, null);
  }

  /**
   * 记录一个拥有一个或多个属性的事件。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和
   * {@link List}，若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param eventName  事件名称
   * @param properties 事件的属性
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void track(String distinctId, boolean isLoginId, String eventName, Map<String, Object> properties)
      throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "track", eventName, properties);
  }

  /**
   * 记录用户注册事件
   *
   * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
   * http://www.sensorsdata.cn/manual/track_signup.html
   * 并在必要时联系我们的技术支持人员。
   *
   * @param loginId       登录 ID
   * @param anonymousId 匿名 ID
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void trackSignUp(String loginId, String anonymousId)
      throws InvalidArgumentException {
    addEvent(loginId, false, anonymousId, "track_signup", "$SignUp", null);
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
   * @param loginId       登录 ID
   * @param anonymousId 匿名 ID
   * @param properties       事件的属性
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void trackSignUp(String loginId, String anonymousId,
      Map<String, Object> properties) throws InvalidArgumentException {
    addEvent(loginId, false, anonymousId, "track_signup", "$SignUp", properties);
  }

  /**
   * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@link List}，
   * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param properties 用户的属性
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileSet(String distinctId, boolean isLoginId, Map<String, Object> properties)
      throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "profile_set", null, properties);
  }

  /**
   * 设置用户的属性。这个接口只能设置单个key对应的内容，同样，如果已经存在，则覆盖，否则，新创建
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileSet(String distinctId, boolean isLoginId, String property, Object value)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    addEvent(distinctId, isLoginId, null, "profile_set", null, properties);
  }

  /**
   * 首次设置用户的属性。
   * 属性取值可接受类型为{@link Number}, {@link String}, {@link Date}和{@link List}，
   * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link Date}类型
   *
   * 与profileSet接口不同的是：
   * 如果要设置的properties的key，在这个用户的profile中已经存在，则不处理，否则，新创建
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param properties 用户的属性
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileSetOnce(String distinctId, boolean isLoginId, Map<String, Object> properties)
      throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "profile_set_once", null, properties);
  }

  /**
   * 首次设置用户的属性。这个接口只能设置单个key对应的内容。
   * 与profileSet接口不同的是，如果key的内容之前已经存在，则不处理，否则，重新创建
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileSetOnce(String distinctId, boolean isLoginId, String property, Object value)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    addEvent(distinctId, isLoginId, null, "profile_set_once", null, properties);
  }

  /**
   * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受
   * {@link Number}类型
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param properties 用户的属性
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileIncrement(String distinctId, boolean isLoginId, Map<String, Object> properties)
      throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "profile_increment", null, properties);
  }

  /**
   * 为用户的数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileIncrement(String distinctId, boolean isLoginId, String property, long value)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, value);
    addEvent(distinctId, isLoginId, null, "profile_increment", null, properties);
  }

  /**
   * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型
   * 必须为 {@link java.lang.String}
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param properties 用户的属性
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileAppend(String distinctId, boolean isLoginId, Map<String, Object> properties)
      throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "profile_append", null, properties);
  }

  /**
   * 为用户的数组类型的属性追加一个字符串
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param property   属性名称
   * @param value      属性的值
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileAppend(String distinctId, boolean isLoginId, String property, String value)
      throws InvalidArgumentException {
    List<String> values = new ArrayList<String>();
    values.add(value);
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, values);
    addEvent(distinctId, isLoginId, null, "profile_append", null, properties);
  }

  /**
   * 删除用户某一个属性
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   * @param property   属性名称
   *
   * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
   */
  public void profileUnset(String distinctId, boolean isLoginId, String property)
      throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<String, Object>();
    properties.put(property, true);
    addEvent(distinctId, isLoginId, null, "profile_unset", null, properties);
  }

  /**
   * 删除用户所有属性
   *
   * @param distinctId 用户 ID
   * @param isLoginId 用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
   *
   * @throws InvalidArgumentException distinctId 不符合命名规范时抛出该异常
   */
  public void profileDelete(String distinctId, boolean isLoginId) throws InvalidArgumentException {
    addEvent(distinctId, isLoginId, null, "profile_delete", null, new HashMap<String, Object>());
  }

  /**
   * 立即发送缓存中的所有日志
   */
  public void flush() {
    this.consumer.flush();
  }

  /**
   * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
   */
  public void shutdown() {
    this.consumer.close();
  }

  private static class HttpConsumer {

    static class HttpConsumerException extends Exception {

      HttpConsumerException(String error, String sendingData, int httpStatusCode, String
          httpContent) {
        super(error);
        this.sendingData = sendingData;
        this.httpStatusCode = httpStatusCode;
        this.httpContent = httpContent;
      }

      String getSendingData() {
        return sendingData;
      }

      int getHttpStatusCode() {
        return httpStatusCode;
      }

      String getHttpContent() {
        return httpContent;
      }

      final String sendingData;
      final int httpStatusCode;
      final String httpContent;
    }

    HttpConsumer(String serverUrl, Map<String, String> httpHeaders) {
      this.serverUrl = serverUrl;
      this.httpHeaders = httpHeaders;

      this.compressData = true;
    }

    HttpResponse consume(final String data) throws IOException, HttpConsumerException {
      HttpResponse response = new DefaultHttpClient().execute(getHttpRequest(data));

      int httpStatusCode = response.getStatusLine().getStatusCode();
      if (httpStatusCode < 200 || httpStatusCode >= 300) {
        String httpContent = EntityUtils.toString(response.getEntity(), "UTF-8");
        throw new HttpConsumerException(String.format("Unexpected response %d from Sensors "
            + "Analytics: %s", httpStatusCode, httpContent), data, httpStatusCode, httpContent);
      }

      return response;
    }

    HttpUriRequest getHttpRequest(final String data) throws IOException {
      HttpPost httpPost = new HttpPost(this.serverUrl);

      httpPost.setEntity(getHttpEntry(data));
      httpPost.addHeader("User-Agent", "SensorsAnalytics Java SDK");

      if (this.httpHeaders != null) {
        for (Map.Entry<String, String> entry : this.httpHeaders.entrySet()) {
          httpPost.addHeader(entry.getKey(), entry.getValue());
        }
      }

      return httpPost;
    }

    UrlEncodedFormEntity getHttpEntry(final String data) throws IOException {
      byte[] bytes = data.getBytes(Charset.forName("UTF-8"));

      List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

      if (compressData) {
        ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
        GZIPOutputStream gos = new GZIPOutputStream(os);
        gos.write(bytes);
        gos.close();
        byte[] compressed = os.toByteArray();
        os.close();

        nameValuePairs.add(new BasicNameValuePair("gzip", "1"));
        nameValuePairs.add(new BasicNameValuePair("data_list", new String(Base64Coder.encode
            (compressed))));
      } else {
        nameValuePairs.add(new BasicNameValuePair("gzip", "0"));
        nameValuePairs.add(new BasicNameValuePair("data_list", new String(Base64Coder.encode
            (bytes))));
      }

      return new UrlEncodedFormEntity(nameValuePairs);
    }

    final String serverUrl;
    final Map<String, String> httpHeaders;

    final Boolean compressData;
  }

  private void addEvent(String distinctId, boolean isLoginId, String originDistinceId,
      String actionType, String eventName, Map<String, Object> properties)
      throws InvalidArgumentException {
    assertKey("Distinct Id", distinctId);
    assertProperties(actionType, properties);
    if (actionType.equals("track")) {
      assertKeyWithRegex("Event Name", eventName);
    } else if (actionType.equals("track_signup")) {
      assertKey("Original Distinct Id", originDistinceId);
    }

    // Event time
    long time = System.currentTimeMillis();
    if (properties != null && properties.containsKey("$time")) {
      Date eventTime = (Date) properties.get("$time");
      properties.remove("$time");
      time = eventTime.getTime();
    }

    Map<String, Object> eventProperties = new HashMap<String, Object>();
    if (actionType.equals("track") || actionType.equals("track_signup")) {
      eventProperties.putAll(superProperties);
    }
    if (properties != null) {
      eventProperties.putAll(properties);
    }

    if (isLoginId) {
      eventProperties.put("$is_login_id", true);
    }

    Map<String, String> libProperties = getLibProperties();

    Map<String, Object> event = new HashMap<String, Object>();

    event.put("type", actionType);
    event.put("time", time);
    event.put("distinct_id", distinctId);
    event.put("properties", eventProperties);
    event.put("lib", libProperties);

    if (enableTimeFree) {
      event.put("time_free", true);
    }

    if (actionType.equals("track")) {
      event.put("event", eventName);
    } else if (actionType.equals("track_signup")) {
      event.put("event", eventName);
      event.put("original_id", originDistinceId);
    }

    this.consumer.send(event);
  }

  private Map<String, String> getLibProperties() {
    Map<String, String> libProperties = new HashMap<String, String>();
    libProperties.put("$lib", "Java");
    libProperties.put("$lib_version", SDK_VERSION);
    libProperties.put("$lib_method", "code");

    if (this.superProperties.containsKey("$app_version")) {
      libProperties.put("$app_version", (String) this.superProperties.get("$app_version"));
    }

    StackTraceElement[] trace = (new Exception()).getStackTrace();

    if (trace.length > 3) {
      StackTraceElement traceElement = trace[3];
      libProperties.put("$lib_detail", String.format("%s##%s##%s##%s", traceElement.getClassName(),
          traceElement.getMethodName(), traceElement.getFileName(), traceElement.getLineNumber()));
    }

    return libProperties;
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
        for (final ListIterator<Object> it = ((List<Object>)property.getValue()).listIterator
            (); it.hasNext();) {
          Object element = it.next();
          if (!(element instanceof String)) {
            throw new InvalidArgumentException("The property value should be a basic type: "
                + "Number, String, Date, Boolean, List<String>.");
          }
          if (((String) element).length() > 8191) {
            it.set(((String) element).substring(0, 8191));
            log.warn(String.format("Element in property '%s' with LIST type is cut off while it's "
                + "too long", (String) element));
          }
        }
      }

      // String 类型的属性值，长度不能超过 8192
      if (property.getValue() instanceof String) {
        String value = (String) property.getValue();
        if (value.length() > 8191) {
          property.setValue(value.substring(0, 8191));
          log.warn(String.format("Property '%s' with STRING type is cut off while it's too long"
              + "."), value);
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

  private static String strJoin(String[] arr, String sep) {
    StringBuilder sbStr = new StringBuilder();
    for (int i = 0, il = arr.length; i < il; i++) {
      if (i > 0)
        sbStr.append(sep);
      sbStr.append(arr[i]);
    }
    return sbStr.toString();
  }

  private static ObjectMapper getJsonObjectMapper() {
    ObjectMapper jsonObjectMapper = new ObjectMapper();
    // 容忍json中出现未知的列
    jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // 兼容java中的驼峰的字段名命名
    jsonObjectMapper.setPropertyNamingStrategy(
        PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    jsonObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    return jsonObjectMapper;
  }

  private final static Logger log = LoggerFactory.getLogger(SensorsAnalytics.class);

  private final static String SDK_VERSION = "3.0.1";

  private final static Pattern KEY_PATTERN = Pattern.compile(
      "^((?!^distinct_id$|^original_id$|^time$|^properties$|^id$|^first_id$|^second_id$|^users$|^events$|^event$|^user_id$|^date$|^datetime$)[a-zA-Z_$][a-zA-Z\\d_$]{0,99})$",
      Pattern.CASE_INSENSITIVE);

  private final Consumer consumer;

  private final Map<String, Object> superProperties;

}
