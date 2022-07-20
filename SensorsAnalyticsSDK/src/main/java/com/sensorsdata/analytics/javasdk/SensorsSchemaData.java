package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.common.Pair;
import com.sensorsdata.analytics.javasdk.common.SchemaTypeEnum;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/14 14:44
 */
class SensorsSchemaData extends SensorsData {

  private String version = SensorsConst.PROTOCOL_VERSION;

  private String schema;
  /**
   * 专门保存 itemEvent /userItem 数据中的 item 信息
   */
  private Pair<String, String> belongItemPair;

  private Long userId;

  private SchemaTypeEnum schemaTypeEnum;

  /**
   * 构建 userEventSchema 数据，actionType:track/bind/unbind
   */
  protected SensorsSchemaData(UserEventSchema userEventSchema, String actionType) {
    super(userEventSchema, actionType);
    this.schema = SensorsConst.USER_EVENT_SCHEMA;
    this.userId = userEventSchema.getUserId();
    this.schemaTypeEnum = SchemaTypeEnum.USER_EVENT;
  }

  protected SensorsSchemaData(ItemSchema itemSchema, String actionType) {
    super(itemSchema.getTrackId(), itemSchema.getItemId(), actionType, null, itemSchema.getProperties());
    this.schema = itemSchema.getSchema();
    this.schemaTypeEnum = SchemaTypeEnum.ITEM;
  }

  protected SensorsSchemaData(ItemEventSchema itemEventSchema, String actionType) {
    super(itemEventSchema.getTrackId(), null, actionType,
        itemEventSchema.getEventName(),
        itemEventSchema.getProperties());
    this.schema = itemEventSchema.getSchema();
    this.belongItemPair = itemEventSchema.getItemPair();
    this.schemaTypeEnum = SchemaTypeEnum.ITEM_EVENT;
  }

  protected SensorsSchemaData(UserSchema userSchema, String actionType) {
    super(userSchema, actionType);
    this.schema = SensorsConst.USER_SCHEMA;
    this.schemaTypeEnum = SchemaTypeEnum.USER;
  }


  protected SensorsSchemaData(UserItemSchema userItemSchema, String actionType) {
    super(userItemSchema.getTrackId(), userItemSchema.getItemId(), actionType, null, userItemSchema.getProperties());
    this.schema = userItemSchema.getSchema();
    this.belongItemPair = userItemSchema.getItemPair();
    this.userId = userItemSchema.getUserId();
    this.schemaTypeEnum = SchemaTypeEnum.USER_ITEM;
  }

  public Map<String, Object> generateData() {
    Map<String, Object> data = new HashMap<>();
    data.put("_track_id", getTrackId());
    data.put("version", version);
    data.put("type", getType());
    data.put("schema", schema);
    data.put("lib", getLib());
    switch (schemaTypeEnum) {
      case ITEM:
        data.put("id", getItemId());
        break;
      case ITEM_EVENT:
        data.put("event", getEvent());
        data.put("time", getTime().getTime());
        getProperties().put(belongItemPair.getKey(), belongItemPair.getValue());
        break;
      case USER:
        if (null != userId) {
          data.put("user_id", userId);
        } else {
          data.put("identities", getIdentities());
          data.put("distinct_id", getDistinctId());
        }
        break;
      case USER_EVENT:
        data.put("event", getEvent());
        data.put("time", getTime().getTime());
        if (null != userId) {
          getProperties().put("user_id", userId);
        } else {
          getProperties().put("identities", getIdentities());
          getProperties().put("distinct_id", getDistinctId());
        }
        break;
      case USER_ITEM:
        data.put("id", getItemId());
        getProperties().put(belongItemPair.getKey(), belongItemPair.getValue());
        if (null != userId) {
          getProperties().put("user_id", userId);
        } else {
          getProperties().put("identities", getIdentities());
          getProperties().put("distinct_id", getDistinctId());
        }
        break;
      default:
        break;
    }
    data.put("properties", getProperties());
    return data;
  }

  public boolean isEventSchemaData() {
    return SchemaTypeEnum.ITEM_EVENT.equals(schemaTypeEnum)
        || SchemaTypeEnum.USER_EVENT.equals(schemaTypeEnum);
  }


  public String getVersion() {
    return version;
  }

  public String getSchema() {
    return schema;
  }

  public Long getUserId() {
    return userId;
  }

  public SchemaTypeEnum getSchemaTypeEnum() {
    return schemaTypeEnum;
  }
}
