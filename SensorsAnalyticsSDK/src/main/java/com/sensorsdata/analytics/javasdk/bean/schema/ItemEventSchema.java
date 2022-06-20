package com.sensorsdata.analytics.javasdk.bean.schema;

import com.sensorsdata.analytics.javasdk.common.Pair;
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
 * @since 2022/06/13 17:03
 */
public class ItemEventSchema {

  private String schema;

  private String eventName;

  private Pair<String, String> itemPair;

  private Map<String, Object> properties;

  private Integer trackId;

  protected ItemEventSchema(Integer trackId, String schema, String eventName, Pair<String, String> itemPair,
      Map<String, Object> properties) {
    this.trackId = trackId;
    this.schema = schema;
    this.eventName = eventName;
    this.itemPair = itemPair;
    this.properties = properties;
  }

  public static IEBuilder init() {
    return new IEBuilder();
  }

  public static class IEBuilder {

    private String schema;
    private String eventName;
    private Pair<String, String> itemPair;
    private Integer trackId;
    private Map<String, Object> properties = new HashMap<>();

    public ItemEventSchema start() throws InvalidArgumentException {
      SensorsAnalyticsUtil.assertKey("event_name", eventName);
      SensorsAnalyticsUtil.assertSchema(schema);
      if (itemPair == null) {
        throw new InvalidArgumentException("Item event must set belong item schema info.");
      }
      SensorsAnalyticsUtil.assertKey("item_schema_key", itemPair.getKey());
      SensorsAnalyticsUtil.assertValue("itemId", itemPair.getValue());
      SensorsAnalyticsUtil.assertSchemaProperties(properties, null);
      this.trackId = SensorsAnalyticsUtil.getTrackId(properties, String.format("[itemPair<%s,%s>,event=%s,schema=%s]",
          itemPair.getKey(), itemPair.getValue(), eventName, schema));
      return new ItemEventSchema(trackId, schema, eventName, itemPair, properties);
    }

    public IEBuilder setSchema(@NonNull String schema) {
      this.schema = schema;
      return this;
    }

    public IEBuilder setEventName(@NonNull String eventName) {
      this.eventName = eventName;
      return this;
    }

    public IEBuilder setItemSchemaPair(@NonNull String itemSchemaKey, @NonNull String itemId) {
      this.itemPair = Pair.of(itemSchemaKey, itemId);
      return this;
    }

    public IEBuilder addProperties(@NonNull Map<String, Object> properties) {
      this.properties.putAll(properties);
      return this;
    }

    public IEBuilder addProperty(@NonNull String key, @NonNull String property) {
      addPropertyObject(key, property);
      return this;
    }

    public IEBuilder addProperty(@NonNull String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public IEBuilder addProperty(@NonNull String key, @NonNull Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public IEBuilder addProperty(@NonNull String key, @NonNull Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public IEBuilder addProperty(@NonNull String key, @NonNull List<String> property) {
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

  public String getEventName() {
    return eventName;
  }

  public Pair<String, String> getItemPair() {
    return itemPair;
  }

  public Map<String, Object> getProperties() {
    return properties;
  }

  public Integer getTrackId() {
    return trackId;
  }
}
