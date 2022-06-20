package com.sensorsdata.analytics.javasdk.bean.schema;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.NonNull;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TODO
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/13 16:53
 */
public class ItemSchema {

  private String schema;

  private String itemId;

  private Map<String, Object> properties;

  private Integer trackId;

  protected ItemSchema(Integer trackId, String schema, String itemId, Map<String, Object> properties) {
    this.trackId = trackId;
    this.schema = schema;
    this.itemId = itemId;
    this.properties = properties;
  }

  public static ISBuilder init() {
    return new ISBuilder();
  }

  public static class ISBuilder {
    private String schema;

    private String itemId;

    private Map<String, Object> properties = new HashMap<>();

    private Integer trackId;

    private ISBuilder() {
    }

    public ItemSchema start() throws InvalidArgumentException {
      if (itemId == null) {
        throw new InvalidArgumentException("The item id can not set null.");
      }
      SensorsAnalyticsUtil.assertValue("itemId", itemId);
      SensorsAnalyticsUtil.assertSchema(schema);
      SensorsAnalyticsUtil.assertSchemaProperties(properties, null);
      this.trackId =
          SensorsAnalyticsUtil.getTrackId(properties, String.format("[itemId=%s,schema=%s]", itemId, schema));
      return new ItemSchema(trackId, schema, itemId, properties);
    }

    public ISBuilder setSchema(@NonNull String schema) {
      this.schema = schema;
      return this;
    }

    public ISBuilder setItemId(@NonNull String itemId) {
      this.itemId = itemId;
      return this;
    }

    public ISBuilder addProperties(@NonNull Map<String, Object> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public ISBuilder addProperty(@NonNull String key, @NonNull String property) {
      addPropertyObject(key, property);
      return this;
    }

    public ISBuilder addProperty(@NonNull String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public ISBuilder addProperty(@NonNull String key, @NonNull Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public ISBuilder addProperty(@NonNull String key, @NonNull Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public ISBuilder addProperty(@NonNull String key, @NonNull List<String> property) {
      addPropertyObject(key, property);
      return this;
    }

    private void addPropertyObject(@NonNull String key, @NonNull Object property) {
      this.properties.put(key, property);
    }

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

  public Integer getTrackId() {
    return trackId;
  }
}
