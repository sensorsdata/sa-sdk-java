package com.sensorsdata.analytics.javasdk.bean;

import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.common.Pair;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.AccessLevel;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.NonNull;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * IDM3.0 事件参数对象，用于构建 IDM3.0 track 请求
 * 使用示例：
 * <p>
 * IDMEventRecord.starter().identityMap(identityMap).setEventName(eventName).addProperties(properties).build();
 * </p>
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/03/09 14:41
 */
@Getter
public class IDMEventRecord {
  /**
   * 事件名称
   */
  private String eventName;
  /**
   * distinctId 标识，在 IDM3.0 里面，该参数可传可不传
   */
  private String distinctId;
  /**
   * 事件携带的属性集合
   */
  private Map<String, Object> propertyMap;

  private Integer trackId;

  private Map<String, String> identityMap;


  protected IDMEventRecord(Map<String, String> identityMap, String eventName, String distinctId,
      Map<String, Object> propertyMap, Integer trackId) {
    this.identityMap = identityMap;
    this.eventName = eventName;
    this.distinctId = distinctId;
    this.propertyMap = propertyMap;
    this.trackId = trackId;
  }

  public static IDMBuilder starter() {
    return new IDMBuilder();
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class IDMBuilder {
    private final Map<String, String> idMap = new LinkedHashMap<>();
    private final Map<String, Object> propertyMap = new HashMap<>();
    private String eventName;
    private String distinctId;
    private Integer trackId;

    public IDMEventRecord build() throws InvalidArgumentException {
      SensorsAnalyticsUtil.assertKey("event_name", eventName);
      if (propertyMap.size() != 0) {
        SensorsAnalyticsUtil.assertProperties(TRACK_ACTION_TYPE, propertyMap);
      }
      if (idMap.size() < 1) {
        throw new InvalidArgumentException("The identity is empty.");
      }
      Pair<String, Boolean> resPair =
          SensorsAnalyticsUtil.checkIdentitiesAndGenerateDistinctId(distinctId, idMap);
      if (resPair.getValue()) {
        propertyMap.put(SensorsConst.LOGIN_SYSTEM_ATTR, true);
      }
      String message = String.format("[distinct_id=%s,event_name=%s]",distinctId,eventName);
      trackId = SensorsAnalyticsUtil.getTrackId(propertyMap, message);
      return new IDMEventRecord(idMap, eventName, resPair.getKey(), propertyMap,trackId);
    }

    public IDMEventRecord.IDMBuilder identityMap(Map<String, String> identityMap) {
      if (identityMap != null) {
        this.idMap.putAll(identityMap);
      }
      return this;
    }

    public IDMEventRecord.IDMBuilder addIdentityProperty(String key, String value) {
      this.idMap.put(key, value);
      return this;
    }

    public IDMEventRecord.IDMBuilder setEventName(@NonNull String eventName) {
      this.eventName = eventName;
      return this;
    }

    public IDMEventRecord.IDMBuilder setDistinctId(@NonNull String distinctId) {
      this.distinctId = distinctId;
      // IDM3.0 设置 distinctId,设置 $is_login_id = false,其实也可不设置
      return this;
    }

    public IDMEventRecord.IDMBuilder addProperties(Map<String, Object> properties) {
      if (properties != null) {
        propertyMap.putAll(properties);
      }
      return this;
    }

    public IDMEventRecord.IDMBuilder addProperty(String key, String property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMEventRecord.IDMBuilder addProperty(String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMEventRecord.IDMBuilder addProperty(String key, Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMEventRecord.IDMBuilder addProperty(String key, Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMEventRecord.IDMBuilder addProperty(String key, List<String> property) {
      addPropertyObject(key, property);
      return this;
    }

    private void addPropertyObject(String key, Object property) {
      if (key != null) {
        propertyMap.put(key, property);
      }
    }


  }
}
