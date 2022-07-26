package com.sensorsdata.analytics.javasdk.bean.schema;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.NonNull;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * user-event schema
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/13 15:28
 */
public class UserEventSchema extends IDMEventRecord {

  private Long userId;

  protected UserEventSchema(Map<String, String> identityMap, String eventName, String distinctId,
      Map<String, Object> propertyMap, Integer trackId, Long userId) {
    super(identityMap, eventName, distinctId, propertyMap, trackId);
    this.userId = userId;
  }

  public Long getUserId() {
    return userId;
  }

  public static UESBuilder init() {
    return new UESBuilder();
  }

  public static class UESBuilder {
    private Map<String, String> idMap = new HashMap<>();
    private Long userId;
    private String distinctId;
    private String eventName;
    private Map<String, Object> properties = new HashMap<>();
    private Integer trackId;

    private UESBuilder() {
    }

    public UserEventSchema start() throws InvalidArgumentException {
      SensorsAnalyticsUtil.assertKey("event_name", eventName);
      if (!properties.isEmpty()) {
        SensorsAnalyticsUtil.assertSchemaProperties(properties, SensorsConst.TRACK_ACTION_TYPE);
      }
      this.trackId = SensorsAnalyticsUtil.getTrackId(properties,
          String.format("[distinct_id=%s,event_name=%s]", distinctId, eventName));
      this.distinctId = SensorsAnalyticsUtil.checkUserInfo(userId, idMap, distinctId);
      return new UserEventSchema(idMap, eventName, distinctId, properties, trackId, userId);
    }

    public UESBuilder setUserId(@NonNull Long userId) {
      this.userId = userId;
      return this;
    }

    public UESBuilder identityMap(@NonNull Map<String, String> identityMap) {
      this.idMap.putAll(identityMap);
      return this;
    }

    public UESBuilder addIdentityProperty(@NonNull String key, @NonNull String value) {
      this.idMap.put(key, value);
      return this;
    }

    public UESBuilder setEventName(@NonNull String eventName) {
      this.eventName = eventName;
      return this;
    }

    public UESBuilder setDistinctId(@NonNull String distinctId) {
      this.distinctId = distinctId;
      // IDM3.0 设置 distinctId,设置 $is_login_id = false,其实也可不设置
      return this;
    }

    public UESBuilder addProperties(@NonNull Map<String, Object> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public UESBuilder addProperty(@NonNull String key, @NonNull String property) {
      addPropertyObject(key, property);
      return this;
    }

    public UESBuilder addProperty(@NonNull String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public UESBuilder addProperty(@NonNull String key, @NonNull Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public UESBuilder addProperty(@NonNull String key, @NonNull Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public UESBuilder addProperty(@NonNull String key, @NonNull List<String> property) {
      addPropertyObject(key, property);
      return this;
    }

    private void addPropertyObject(@NonNull String key, @NonNull Object property) {
      this.properties.put(key, property);
    }


  }
}
