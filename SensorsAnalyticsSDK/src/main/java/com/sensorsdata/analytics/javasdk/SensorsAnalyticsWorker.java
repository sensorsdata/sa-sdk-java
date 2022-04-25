package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.APP_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.BIND_ID_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LOGIN_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROJECT_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.SDK_VERSION;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TIME_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TOKEN_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_SIGN_UP_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.UNBIND_ID_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.Consumer;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
class SensorsAnalyticsWorker {

  private final Consumer consumer;

  private final Map<String, Object> superProperties = new ConcurrentHashMap<String, Object>();

  private boolean enableTimeFree = false;

  public SensorsAnalyticsWorker(Consumer consumer) {
    this.consumer = consumer;
    clearSuperProperties();
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        log.info("Triggered flush when the program is closed.");
        flush();
      }
    }));
  }

  void setEnableTimeFree(boolean enableTimeFree) {
    log.info("Call setEnableTimeFree method with param:{}", enableTimeFree);
    this.enableTimeFree = enableTimeFree;
  }

  void setSuperProperties(Map<String, Object> superProperties) {
    this.superProperties.putAll(superProperties);
  }

  void clearSuperProperties() {
    this.superProperties.clear();
    this.superProperties.put(LIB_SYSTEM_ATTR, LIB);
    this.superProperties.put(LIB_VERSION_SYSTEM_ATTR, SDK_VERSION);
    log.info("Call clearSuperProperties method.");
  }

  void doAddEvent(String distinctId, boolean isLoginId, String originDistinctId, String actionType, String eventName,
      Map<String, Object> properties) {
    this.consumer.send(
        generateEventMap(distinctId, isLoginId, originDistinctId, null, actionType, eventName, properties));
  }

  void doAddData(@NonNull SensorsData sensorsData) {
    //enable history data import
    if (enableTimeFree) {
      sensorsData.getProperties().put("time_free", true);
    }
    //check properties
    final Map<String, Object> properties = sensorsData.getProperties();
    if (properties.containsKey(PROJECT_SYSTEM_ATTR)) {
      sensorsData.setProject(properties.get(PROJECT_SYSTEM_ATTR).toString());
      properties.remove(PROJECT_SYSTEM_ATTR);
    }
    if (properties.containsKey(TOKEN_SYSTEM_ATTR)) {
      sensorsData.setToken(properties.get(TOKEN_SYSTEM_ATTR).toString());
      properties.remove(TOKEN_SYSTEM_ATTR);
    }
    if (properties.containsKey(TIME_SYSTEM_ATTR)) {
      sensorsData.setTime((Date) properties.get(TIME_SYSTEM_ATTR));
      properties.remove(TIME_SYSTEM_ATTR);
    }
    //check common properties contains $app_version
    if (this.superProperties.containsKey(APP_VERSION_SYSTEM_ATTR)) {
      sensorsData.getLib().put(APP_VERSION_SYSTEM_ATTR, (String) this.superProperties.get(APP_VERSION_SYSTEM_ATTR));
    }
    // 只有 track 和 track_signup 事件才需要设置公共属性
    if (sensorsData.getType().startsWith(TRACK_ACTION_TYPE)) {
      properties.putAll(superProperties);
    }
    //event or profile
    this.consumer.send(SensorsData.generateData(sensorsData));
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
   * 非 IDM 模式下 identity 一定为 null
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
      if (distinctId != null) {
        eventMap.put("distinct_id", distinctId);
        eventProperties.put(LOGIN_SYSTEM_ATTR, false);
      } else {
        if (identity.containsKey(SensorsAnalyticsIdentity.LOGIN_ID)) {
          eventProperties.put(LOGIN_SYSTEM_ATTR, true);
          eventMap.put("distinct_id", identity.get(SensorsAnalyticsIdentity.LOGIN_ID));
        } else {
          eventProperties.put(LOGIN_SYSTEM_ATTR, false);
          String firstKey = identity.keySet().iterator().next();
          eventMap.put("distinct_id", String.format("%s+%s", firstKey, identity.get(firstKey)));
        }
      }
    }
    //检查自定义属性
    if (properties != null && !properties.isEmpty()) {
      for (Map.Entry<String, Object> entry : properties.entrySet()) {
        switch (entry.getKey()) {
          case TIME_SYSTEM_ATTR:
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
    Map<String, String> libInfo = SensorsAnalyticsUtil.generateLibInfo();
    if (this.superProperties.containsKey(APP_VERSION_SYSTEM_ATTR)) {
      libInfo.put(APP_VERSION_SYSTEM_ATTR, (String) this.superProperties.get(APP_VERSION_SYSTEM_ATTR));
    }
    return libInfo;
  }
}
