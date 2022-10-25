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
 * user-item schema
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/13 16:07
 */
@Getter
public class UserItemSchema {

  private Integer trackId;

  private Long userId;

  private String distinctId;

  private String schema;

  private String itemId;

  private Map<String, Object> properties;

  private Map<String, String> identityMap;


  protected UserItemSchema(Map<String, String> idMap, String schema, String itemId,
      Map<String, Object> properties, Integer trackId, Long userId, String distinctId) {
    this.identityMap = idMap;
    this.schema = schema;
    this.itemId = itemId;
    this.properties = properties;
    this.trackId = trackId;
    this.userId = userId;
    this.distinctId = distinctId;
  }

  public static UISBuilder init() {
    return new UISBuilder();
  }

  public static class UISBuilder {
    private Integer trackId;
    private Long userId;
    private Map<String, String> idMap = new HashMap<>();
    private String schema;
    private String itemId;
    private Map<String, Object> properties = new HashMap<>();
    private String distinctId;

    private UISBuilder() {
    }

    public UserItemSchema start() throws InvalidArgumentException {
      this.distinctId = SensorsAnalyticsUtil.checkUserInfo(userId, idMap, distinctId);
      SensorsAnalyticsUtil.assertValue("item_id", itemId);
      SensorsAnalyticsUtil.assertSchema(schema);
      this.trackId = SensorsAnalyticsUtil.getTrackId(properties,
          String.format("user item event generate trackId error.[distinct_id=%s,user_id=%s,item_id=%s,schema=%s]",
              distinctId, userId, itemId, schema));
      SensorsAnalyticsUtil.assertSchemaProperties(properties, null);
      return new UserItemSchema(idMap, schema, itemId, properties, trackId, userId, distinctId);
    }

    public UISBuilder setUserId(@NonNull Long userId) {
      this.userId = userId;
      return this;
    }

    public UISBuilder setItemId(@NonNull String itemId) {
      this.itemId = itemId;
      return this;
    }

    public UISBuilder setSchema(@NonNull String schema) {
      this.schema = schema;
      return this;
    }

    public UISBuilder identityMap(@NonNull Map<String, String> identityMap) {
      this.idMap.putAll(identityMap);
      return this;
    }

    public UISBuilder addIdentityProperty(@NonNull String key, @NonNull String value) {
      this.idMap.put(key, value);
      return this;
    }


    public UISBuilder setDistinctId(@NonNull String distinctId) {
      this.distinctId = distinctId;
      return this;
    }

    public UISBuilder addProperties(@NonNull Map<String, Object> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public UISBuilder addProperty(@NonNull String key, @NonNull String property) {
      addPropertyObject(key, property);
      return this;
    }

    public UISBuilder addProperty(@NonNull String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public UISBuilder addProperty(@NonNull String key, @NonNull Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public UISBuilder addProperty(@NonNull String key, @NonNull Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public UISBuilder addProperty(@NonNull String key, @NonNull List<String> property) {
      addPropertyObject(key, property);
      return this;
    }

    private void addPropertyObject(@NonNull String key, @NonNull Object property) {
      this.properties.put(key, property);
    }
  }

  public Integer getTrackId() {
    return trackId;
  }

  public Long getUserId() {
    return userId;
  }

  public String getDistinctId() {
    return distinctId;
  }

  public String getSchema() {
    return schema;
  }

  public String getItemId() {
    return itemId;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }
}
