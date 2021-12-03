package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

import static com.sensorsdata.analytics.javasdk.SensorsConst.*;


class SensorsAnalyticsWorker {

  private final Consumer consumer;

  private final Map<String, Object> superProperties;

  private boolean enableTimeFree = false;

  public SensorsAnalyticsWorker(Consumer consumer) {
    this.consumer = consumer;
    this.superProperties = new ConcurrentHashMap<String, Object>();
    clearSuperProperties();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        flush();
      }
    }));
  }

  void setEnableTimeFree(boolean enableTimeFree) {
    this.enableTimeFree = enableTimeFree;
  }

  void setSuperProperties(Map<String, Object> superProperties) {
    this.superProperties.putAll(superProperties);
  }

  void clearSuperProperties() {
    this.superProperties.clear();
    this.superProperties.put(LIB_SYSTEM_ATTR, LIB);
    this.superProperties.put(LIB_VERSION_SYSTEM_ATTR, SDK_VERSION);
  }

  void doAddEvent(String distinctId, boolean isLoginId, String originDistinctId, String actionType, String eventName,
      Map<String, Object> properties) {
    this.consumer.send(
        generateEventMap(distinctId, isLoginId, originDistinctId, null, actionType, eventName, properties));
  }

  void doAddItem(String itemType, String itemId, String actionType, Map<String, Object> properties) {
    Map<String, Object> item = new HashMap<>();
    item.put("item_type", itemType);
    item.put("item_id", itemId);
    item.put("type", actionType);
    item.put("time", System.currentTimeMillis());
    item.put("lib", getLibProperties());
    Map<String, Object> itemProperties = new HashMap<>();
    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        switch (entry.getKey()) {
          case PROJECT_SYSTEM_ATTR:
            item.put("project", entry.getValue());
            break;
          case TOKEN_SYSTEM_ATTR:
            item.put("token", entry.getValue());
            break;
          default:
            itemProperties.put(entry.getKey(), entry.getValue());
            break;
        }
      }
    }
    item.put("properties", itemProperties);
    this.consumer.send(item);
  }

  void doAddEventIdentity(Map<String, String> identity, String actionType, String eventName,
      Map<String, Object> properties) {
    this.consumer.send(generateEventMap(null, null, null, identity, actionType, eventName, properties));
  }

  void flush() {
    this.consumer.flush();
  }

  void shutdown() {
    this.consumer.close();
  }

  /**
   * distinctId、isLoginId、originDistinctId 与 identity 作为互斥参数，同时只有一种有值
   *
   * @param distinctId       登录 ID
   * @param isLoginId        是否登录ID
   * @param originDistinctId 匿名ID
   * @param identity         ID-Mapping 身份标识
   * @param actionType       行为类型
   * @param eventName        事件名
   * @param properties       属性
   * @return Map<String, Object>
   */
  private Map<String, Object> generateEventMap(String distinctId, Boolean isLoginId, String originDistinctId,
      Map<String, String> identity, String actionType, String eventName, Map<String, Object> properties) {
    Map<String, Object> eventMap = new HashMap<>();
    eventMap.put("_track_id", new Random().nextInt());
    eventMap.put("type", actionType);
    eventMap.put("lib", getLibProperties());
    //开启历史数据导入
    if (enableTimeFree) {
      eventMap.put("time_free", true);
    }
    HashMap<String, Object> eventProperties = new HashMap<>();
    //普通模式
    if (identity == null) {
      eventMap.put("distinct_id", distinctId);
      if (isLoginId) {
        eventProperties.put(LOGIN_SYSTEM_ATTR, true);
      }
    } else {// id-mapping 模式
      eventMap.put("identities", identity);
      if (identity.containsKey(SensorsAnalyticsIdentity.LOGIN_ID)) {
        eventProperties.put(LOGIN_SYSTEM_ATTR, true);
        eventMap.put("distinct_id", identity.get(SensorsAnalyticsIdentity.LOGIN_ID));
      } else {
        eventProperties.put(LOGIN_SYSTEM_ATTR, false);
        eventMap.put("distinct_id", identity.get(identity.keySet().iterator().next()));
      }
    }
    //检查自定义属性
    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        switch (entry.getKey()) {
          case TINE_SYSTEM_ATTR:
            eventMap.put("time", ((Date) entry.getValue()).getTime());
            break;
          case PROJECT_SYSTEM_ATTR:
            eventMap.put("project", entry.getValue());
            break;
          case TOKEN_SYSTEM_ATTR:
            eventMap.put("token", entry.getValue());
            break;
          default:
            eventProperties.put(entry.getKey(), entry.getValue());
            break;
        }
      }
    }
    //操作类型
    if (actionType != null) {
      switch (actionType) {
        case TRACK_SIGN_UP_ACTION_TYPE:
          eventMap.put("original_id", originDistinctId);
        case TRACK_ACTION_TYPE:
        case BIND_ID_ACTION_TYPE:
        case UNBIND_ID_ACTION_TYPE:
          eventMap.put("event", eventName);
          eventProperties.putAll(superProperties);
          break;
      }
    }
    //最终校验是否有 time 属性
    if (!eventMap.containsKey("time")) {
      eventMap.put("time", System.currentTimeMillis());
    }
    eventMap.put("properties", eventProperties);
    return eventMap;
  }

  private Map<String, String> getLibProperties() {
    Map<String, String> libProperties = new HashMap<>();
    libProperties.put(LIB_SYSTEM_ATTR, LIB);
    libProperties.put(LIB_VERSION_SYSTEM_ATTR, SDK_VERSION);
    libProperties.put(LIB_METHOD_SYSTEM_ATTR, "code");

    if (this.superProperties.containsKey(APP_VERSION_SYSTEM_ATTR)) {
      libProperties.put(APP_VERSION_SYSTEM_ATTR, (String) this.superProperties.get(APP_VERSION_SYSTEM_ATTR));
    }

    StackTraceElement[] trace = (new Exception()).getStackTrace();

    if (trace.length > 3) {
      StackTraceElement traceElement = trace[3];
      libProperties.put(LIB_DETAIL_SYSTEM_ATTR,
          String.format("%s##%s##%s##%s", traceElement.getClassName(), traceElement.getMethodName(),
              traceElement.getFileName(), traceElement.getLineNumber()));
    }
    return libProperties;
  }
}
