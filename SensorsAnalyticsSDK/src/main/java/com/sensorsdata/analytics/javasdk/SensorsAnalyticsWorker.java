package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.BIND_ID_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.DEFAULT_LIB_DETAIL;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_DETAIL_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_METHOD_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.SDK_VERSION;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_SIGN_UP_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.UNBIND_ID_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.consumer.Consumer;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

@Slf4j
class SensorsAnalyticsWorker {

  private final Consumer consumer;

  private boolean timeFree = false;

  private boolean enableCollectMethodStack = true;

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
    data.put("lib", generateLibInfo());
    if (timeFree && (TRACK_ACTION_TYPE.equals(sensorsData.getType())
        || TRACK_SIGN_UP_ACTION_TYPE.equals(sensorsData.getType()))
        || BIND_ID_ACTION_TYPE.equals(sensorsData.getType())
        || UNBIND_ID_ACTION_TYPE.equals(sensorsData.getType())) {
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
    sensorsData.put("lib", generateLibInfo());
    if (timeFree && (TRACK_ACTION_TYPE.equals(schemaData.getType())
        || TRACK_SIGN_UP_ACTION_TYPE.equals(schemaData.getType())
        || BIND_ID_ACTION_TYPE.equals(schemaData.getType())
        || UNBIND_ID_ACTION_TYPE.equals(schemaData.getType()))) {
      sensorsData.put("time_free", true);
    }
    this.consumer.send(sensorsData);
  }

  public void setEnableTimeFree(boolean enableTimeFree) {
    this.timeFree = enableTimeFree;
  }

  public void setEnableCollectMethodStack(boolean enableCollectMethodStack) {
    this.enableCollectMethodStack = enableCollectMethodStack;
  }

  public Map<String, String> generateLibInfo() {
    Map<String, String> libProperties = new HashMap<>();
    libProperties.put(LIB_SYSTEM_ATTR, LIB);
    libProperties.put(LIB_VERSION_SYSTEM_ATTR, SDK_VERSION);
    libProperties.put(LIB_METHOD_SYSTEM_ATTR, "code");
    if (enableCollectMethodStack) {
      StackTraceElement[] trace = (new Exception()).getStackTrace();
      if (trace.length > 3) {
        StackTraceElement traceElement = trace[3];
        libProperties.put(LIB_DETAIL_SYSTEM_ATTR,
            String.format("%s##%s##%s##%s", traceElement.getClassName(), traceElement.getMethodName(),
                traceElement.getFileName(), traceElement.getLineNumber()));
      }
    } else {
      libProperties.put(LIB_DETAIL_SYSTEM_ATTR, DEFAULT_LIB_DETAIL);
    }
    return libProperties;
  }
}
