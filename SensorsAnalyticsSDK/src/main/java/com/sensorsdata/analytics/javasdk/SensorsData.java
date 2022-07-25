package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.Getter;
import lombok.Setter;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * 神策数据格式
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/03/09 16:16
 */
@Getter
@Setter
class SensorsData {
  /**
   * 数据唯一标识
   */
  private Integer trackId;
  /**
   * 数据用户唯一标识
   */
  private String distinctId;
  /**
   * 数据用户匿名标识
   */
  private String originalId;
  /**
   * IDM3.0 用户维度标识
   */
  private Map<String, String> identities;
  /**
   * 事件类型
   */
  private String type;
  /**
   * 事件名称
   */
  private String event;
  /**
   * 数据采集来源
   */
  private Map<String, String> lib;
  /**
   * 记录生成时间
   */
  private Date time;
  /**
   * 数据属性集合
   */
  private Map<String, Object> properties;
  /**
   * 数据接收项目
   */
  private String project;
  /**
   * 数据 token
   */
  private String token;
  /**
   * 纬度类型
   */
  private String itemType;
  /**
   * 纬度 ID
   */
  private String itemId;

  protected SensorsData() {
  }

  protected SensorsData(EventRecord eventRecord, String actionType) {
    this(eventRecord.getDistinctId(), eventRecord.getOriginalId(), null, actionType, eventRecord.getEventName(),
        eventRecord.getPropertyMap(), null, null, eventRecord.getTrackId());
  }

  protected SensorsData(ItemRecord itemRecord, String actionType) {
    this(null, null, null, actionType, null, itemRecord.getPropertyMap(),
        itemRecord.getItemType(), itemRecord.getItemId(), itemRecord.getTrackId());
  }

  protected SensorsData(UserRecord userRecord, String actionType) {
    this(userRecord.getDistinctId(), actionType, null, userRecord.getPropertyMap(), userRecord.getTrackId());
  }

  protected <T extends IDMUserRecord> SensorsData(T userRecord, String actionType) {
    this(userRecord.getDistinctId(), actionType, userRecord.getIdentityMap(), userRecord.getPropertyMap(),
        userRecord.getTrackId());
  }

  protected <T extends IDMEventRecord> SensorsData(T eventRecord) {
    this(eventRecord.getDistinctId(), eventRecord.getIdentityMap(), eventRecord.getEventName(),
        eventRecord.getPropertyMap(), eventRecord.getTrackId());
  }

  protected <T extends IDMEventRecord> SensorsData(T eventRecord, String actionType) {
    this(eventRecord.getDistinctId(), null, eventRecord.getIdentityMap(), actionType,
        eventRecord.getEventName(), eventRecord.getPropertyMap(), null, null, eventRecord.getTrackId());
  }

  protected SensorsData(String distinctId, String type, Map<String, String> identities,
      Map<String, Object> properties, Integer trackId) {
    this(distinctId, null, identities, type, null, properties, null, null, trackId);
  }

  /**
   * 构建 track 数据实体
   *
   * @param distinctId 用户ID
   * @param event      事件名
   * @param properties 事件属性集合
   */
  protected SensorsData(String distinctId, Map<String, String> identities, String event,
      Map<String, Object> properties, Integer trackId) {
    this(distinctId, null, identities, TRACK_ACTION_TYPE, event, properties, null, null, trackId);
  }

  /**
   * 专用于 item 相关 schema 构建
   */
  protected SensorsData(Integer trackId, String itemId, String type, String event, Map<String, Object> properties) {
    this(null, null, null, type, event, properties, null, itemId, trackId);
  }

  private SensorsData(String distinctId, String originalId, Map<String, String> identities, String type, String event,
      Map<String, Object> properties, String itemType, String itemId, Integer trackId) {
    this.trackId = trackId;
    this.distinctId = distinctId;
    this.originalId = originalId;
    this.identities = identities;
    this.type = type;
    this.event = event;
    this.lib = SensorsAnalyticsUtil.generateLibInfo();
    this.time = properties.containsKey(SensorsConst.TIME_SYSTEM_ATTR) ?
        (Date) properties.get(SensorsConst.TIME_SYSTEM_ATTR) : new Date();
    this.properties = properties;
    this.itemType = itemType;
    this.itemId = itemId;
    this.project = properties.get(SensorsConst.PROJECT_SYSTEM_ATTR) == null ?
        null : String.valueOf(properties.get(SensorsConst.PROJECT_SYSTEM_ATTR));
    this.token = properties.get(SensorsConst.TOKEN_SYSTEM_ATTR) == null ?
        null : String.valueOf(properties.get(SensorsConst.TOKEN_SYSTEM_ATTR));
  }


  protected static Map<String, Object> generateData(SensorsData sensorsData) {
    Map<String, Object> eventMap = new HashMap<>();
    if (sensorsData.getTrackId() != null) {
      eventMap.put("_track_id", sensorsData.getTrackId());
    }
    if (sensorsData.getDistinctId() != null) {
      eventMap.put("distinct_id", sensorsData.getDistinctId());
    }
    if (sensorsData.getOriginalId() != null) {
      eventMap.put("original_id", sensorsData.getOriginalId());
    }
    if (sensorsData.getIdentities() != null) {
      eventMap.put("identities", sensorsData.getIdentities());
    }
    if (sensorsData.getType() != null) {
      eventMap.put("type", sensorsData.getType());
    }
    if (sensorsData.getEvent() != null) {
      eventMap.put("event", sensorsData.getEvent());
    }
    if (sensorsData.getLib() != null) {
      eventMap.put("lib", sensorsData.getLib());
    }
    // fix 【SDK-4709】time 类型保持为时间戳类型
    eventMap.put("time", sensorsData.getTime().getTime());
    if (sensorsData.getProject() != null) {
      eventMap.put("project", sensorsData.getProject());
    }
    if (sensorsData.getToken() != null) {
      eventMap.put("token", sensorsData.getToken());
    }
    if (sensorsData.getItemId() != null) {
      eventMap.put("item_id", sensorsData.getItemId());
    }
    if (sensorsData.getItemType() != null) {
      eventMap.put("item_type", sensorsData.getItemType());
    }
    if (sensorsData.getProperties() != null) {
      eventMap.put("properties", sensorsData.getProperties());
    }
    return eventMap;
  }

}
