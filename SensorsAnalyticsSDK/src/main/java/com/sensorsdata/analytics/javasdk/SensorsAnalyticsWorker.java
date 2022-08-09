package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.APP_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROJECT_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.SDK_VERSION;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TIME_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TOKEN_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Date;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
class SensorsAnalyticsWorker {

  private final Consumer consumer;

  private final Map<String, Object> superProperties = new ConcurrentHashMap<>();

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
      for (Map.Entry<String, Object> entry : superProperties.entrySet()) {
        if (!properties.containsKey(entry.getKey())) {
          properties.put(entry.getKey(), entry.getValue());
        }
      }
    }
    //event or profile
    this.consumer.send(SensorsData.generateData(sensorsData));
  }

  void flush() {
    this.consumer.flush();
  }

  void shutdown() {
    this.consumer.close();
  }

}
