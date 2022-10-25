package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.common.Pair;
import com.sensorsdata.analytics.javasdk.common.SchemaTypeEnum;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;

/**
 * TODO
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/14 14:44
 */
@Slf4j
class SensorsSchemaData extends SensorsData {

  private String version = SensorsConst.PROTOCOL_VERSION;

  private String schema;

  private Long userId;

  private Pair<String, String> itemEventPair;

  private SchemaTypeEnum schemaTypeEnum;

  /**
   * 构建 userEventSchema 数据，actionType:track/bind/unbind
   */
  protected SensorsSchemaData(UserEventSchema userEventSchema, String actionType) {
    super(userEventSchema.getDistinctId(), null, userEventSchema.getIdentityMap(), actionType,
        userEventSchema.getEventName(), userEventSchema.getPropertyMap(), null, null, userEventSchema.getTrackId());
    this.schema = SensorsConst.USER_EVENT_SCHEMA;
    this.userId = userEventSchema.getUserId();
    this.schemaTypeEnum = SchemaTypeEnum.USER_EVENT;
  }

  protected SensorsSchemaData(ItemSchema itemSchema, String actionType) {
    super(itemSchema.getTrackId(), null, null, itemSchema.getItemId(), actionType, null, itemSchema.getProperties());
    this.schema = itemSchema.getSchema();
    this.schemaTypeEnum = SchemaTypeEnum.ITEM;
  }

  protected SensorsSchemaData(ItemEventSchema itemEventSchema, String actionType) {
    super(itemEventSchema.getTrackId(), null, null, null, actionType,
        itemEventSchema.getEventName(),
        itemEventSchema.getProperties());
    this.schema = itemEventSchema.getSchema();
    this.itemEventPair = itemEventSchema.getItemPair();
    this.schemaTypeEnum = SchemaTypeEnum.ITEM_EVENT;
  }

  protected SensorsSchemaData(UserSchema userSchema, String actionType) {
    super(userSchema.getDistinctId(), actionType, userSchema.getIdentityMap(), userSchema.getPropertyMap(),
        userSchema.getTrackId());
    this.schema = SensorsConst.USER_SCHEMA;
    this.userId = userSchema.getUserId();
    this.schemaTypeEnum = SchemaTypeEnum.USER;
  }


  protected SensorsSchemaData(UserItemSchema userItemSchema, String actionType) {
    super(userItemSchema.getTrackId(), userItemSchema.getDistinctId(), userItemSchema.getIdentityMap(),
        userItemSchema.getItemId(), actionType, null, userItemSchema.getProperties());
    this.schema = userItemSchema.getSchema();
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
    data.put("time", getTime().getTime());
    if (getProject() != null && !"".equals(getProject())) {
      data.put("project", getProject());
    }
    if (getToken() != null && !"".equals(getToken())) {
      data.put("token", getToken());
    }
    switch (schemaTypeEnum) {
      case ITEM:
        data.put("id", getItemId());
        break;
      case ITEM_EVENT:
        getProperties().put(itemEventPair.getKey(), itemEventPair.getValue());
        data.put("event", getEvent());
        break;
      case USER:
        checkUserIdAndAddUser(data, "id");
        break;
      case USER_EVENT:
        data.put("event", getEvent());
        checkUserIdAndAddUser(getProperties(), "user_id");
        break;
      case USER_ITEM:
        data.put("id", getItemId());
        checkUserIdAndAddUser(getProperties(), "user_id");
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


  private void checkUserIdAndAddUser(Map<String, Object> data, String key) {
    if (null != getUserId()) {
      data.put(key, getUserId());
    } else {
      data.put("identities", getIdentities());
    }
    data.put("distinct_id", getDistinctId());
  }
}
