package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_SIGN_UP_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.Map;

@Slf4j
class SensorsAnalyticsWorker {

  private final Consumer consumer;

  private boolean timeFree = false;

  public SensorsAnalyticsWorker(Consumer consumer) {
    this.consumer = consumer;
    Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
      @Override
      public void run() {
        log.info("Triggered flush when the program is closed.");
        flush();
      }
    }));
  }

  void doAddData(@NonNull SensorsData sensorsData) {
    Map<String, Object> data = SensorsData.generateData(sensorsData);
    if (timeFree && (TRACK_ACTION_TYPE.equals(sensorsData.getType()))
        || TRACK_SIGN_UP_ACTION_TYPE.equals(sensorsData.getType())) {
      data.put("time_free", true);
    }
    this.consumer.send(data);
  }

  void flush() {
    this.consumer.flush();
  }

  void shutdown() {
    this.consumer.close();
  }


  public void doSchemaData(@NonNull SensorsSchemaData schemaData) {
    Map<String, Object> sensorsData = schemaData.generateData();
    if (timeFree && (TRACK_ACTION_TYPE.equals(schemaData.getType()))
        || TRACK_SIGN_UP_ACTION_TYPE.equals(schemaData.getType())) {
      sensorsData.put("time_free", true);
    }
    this.consumer.send(sensorsData);
  }

  public void setEnableTimeFree(boolean enableTimeFree) {
    this.timeFree = enableTimeFree;
  }
}
