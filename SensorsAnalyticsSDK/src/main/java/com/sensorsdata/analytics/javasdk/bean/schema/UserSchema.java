package com.sensorsdata.analytics.javasdk.bean.schema;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.Getter;
import lombok.NonNull;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用户相关 schema
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/13 15:49
 */
@Getter
public class UserSchema {

  private Long userId;

  private Map<String, Object> propertyMap;

  private String distinctId;

  private Integer trackId;

  private Map<String, String> identityMap;

  protected UserSchema(Map<String, String> identityMap, Map<String, Object> propertyMap, String distinctId,
      Integer trackId, Long userId) {
    this.userId = userId;
    this.propertyMap = propertyMap;
    this.distinctId = distinctId;
    this.trackId = trackId;
    this.identityMap = identityMap;
  }

  public static USBuilder init() {
    return new USBuilder();
  }

  public static class USBuilder {
    private Map<String, String> idMap = new HashMap<>();
    private String distinctId;
    private Integer trackId;
    private Long userId;
    private Map<String, Object> properties = new HashMap<>();

    private USBuilder() {
    }

    public UserSchema start() throws InvalidArgumentException {
      this.distinctId = SensorsAnalyticsUtil.checkUserInfo(userId, idMap, distinctId);
      this.trackId =
          SensorsAnalyticsUtil.getTrackId(properties, String.format("[distinct_id=%s,user_id=%s]", distinctId, userId));
      return new UserSchema(idMap, properties, distinctId, trackId, userId);
    }

    public USBuilder identityMap(@NonNull Map<String, String> identityMap) {
      this.idMap.putAll(identityMap);
      return this;
    }

    public USBuilder addIdentityProperty(@NonNull String key, @NonNull String value) {
      this.idMap.put(key, value);
      return this;
    }


    public USBuilder setDistinctId(@NonNull String distinctId) {
      this.distinctId = distinctId;
      // IDM3.0 设置 distinctId,设置 $is_login_id = false,其实也可不设置
      return this;
    }

    public USBuilder addProperties(@NonNull Map<String, Object> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public USBuilder addProperty(@NonNull String key, @NonNull String property) {
      addPropertyObject(key, property);
      return this;
    }

    public USBuilder addProperty(@NonNull String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public USBuilder addProperty(@NonNull String key, @NonNull Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public USBuilder addProperty(@NonNull String key, @NonNull Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public USBuilder addProperty(@NonNull String key, @NonNull List<String> property) {
      addPropertyObject(key, property);
      return this;
    }

    private void addPropertyObject(@NonNull String key, @NonNull Object property) {
      this.properties.put(key, property);
    }

  }

  public Long getUserId() {
    return userId;
  }

}
