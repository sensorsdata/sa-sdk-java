package com.sensorsdata.analytics.javasdk.bean;

import static com.sensorsdata.analytics.javasdk.SensorsConst.LOGIN_SYSTEM_ATTR;

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
 * IDM3.0 用户参数信息对象，用于构建 IDM3.0 profile 相关接口请求
 * 使用示例：
 * <p>
 * IDMUserRecord.starter().identityMap(identityMap).addProperties(properties).build();
 * </p>
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/03/09 14:44
 */
@Getter
public class IDMUserRecord extends SensorsAnalyticsIdentity {

  private final Map<String, Object> propertyMap;

  private final String distinctId;

  private IDMUserRecord(Map<String, String> identityMap, Map<String, Object> propertyMap, String distinctId) {
    super(identityMap);
    this.propertyMap = propertyMap;
    this.distinctId = distinctId;
  }

  public static IDMBuilder starter() {
    return new IDMBuilder();
  }

  @NoArgsConstructor(access = AccessLevel.PRIVATE)
  public static class IDMBuilder {
    private final Map<String, String> idMap = new LinkedHashMap<>();
    private String distinctId;
    private final Map<String, Object> propertyMap = new HashMap<>();

    public IDMUserRecord build() throws InvalidArgumentException {
      Pair<String, Boolean> resPair =
          SensorsAnalyticsUtil.checkIdentitiesAndGenerateDistinctId(distinctId, idMap);
      propertyMap.put(LOGIN_SYSTEM_ATTR, resPair.getValue());
      return new IDMUserRecord(idMap, propertyMap, resPair.getKey());
    }

    public IDMUserRecord.IDMBuilder identityMap(Map<String, String> identityMap) {
      if (identityMap != null) {
        this.idMap.putAll(identityMap);
      }
      return this;
    }

    public IDMUserRecord.IDMBuilder addIdentityProperty(String key, String value) {
      this.idMap.put(key, value);
      return this;
    }

    public IDMUserRecord.IDMBuilder setDistinctId(@NonNull String distinctId) {
      this.distinctId = distinctId;
      // IDM3.0 设置 distinctId,设置 $is_login_id = false,其实也可不设置
      propertyMap.put(LOGIN_SYSTEM_ATTR, false);
      return this;
    }

    public IDMUserRecord.IDMBuilder addProperties(Map<String, Object> properties) {
      if (properties != null) {
        propertyMap.putAll(properties);
      }
      return this;
    }

    public IDMUserRecord.IDMBuilder addProperty(String key, String property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMUserRecord.IDMBuilder addProperty(String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMUserRecord.IDMBuilder addProperty(String key, Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMUserRecord.IDMBuilder addProperty(String key, Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public IDMUserRecord.IDMBuilder addProperty(String key, List<String> property) {
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
