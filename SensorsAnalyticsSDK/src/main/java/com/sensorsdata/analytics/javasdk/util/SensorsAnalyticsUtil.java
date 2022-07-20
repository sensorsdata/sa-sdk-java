package com.sensorsdata.analytics.javasdk.util;

import static com.sensorsdata.analytics.javasdk.SensorsConst.BIND_ID_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_DELETE_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_SET_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_DETAIL_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_METHOD_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.LIB_VERSION_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_APPEND_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_DELETE_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_INCREMENT_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_SET_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_SET_ONCE_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_UNSET_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.SDK_VERSION;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_SIGN_UP_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.UNBIND_ID_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.common.Pair;
import com.sensorsdata.analytics.javasdk.common.SensorsDataTypeEnum;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import lombok.extern.slf4j.Slf4j;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.Random;
import java.util.TimeZone;
import java.util.regex.Pattern;

@Slf4j
public class SensorsAnalyticsUtil {
  private static final Pattern KEY_PATTERN = Pattern.compile(
      "^((?!^distinct_id$|^original_id$|^time$|^properties$|^id$|^first_id$|^second_id$|^users$|^events$|^event$|^user_id$|^date$|^datetime$|^user_group|^user_tag)[a-zA-Z_$][a-zA-Z\\d_$]{0,99})$",
      Pattern.CASE_INSENSITIVE);

  public static String strJoin(String[] arr, String sep) {
    StringBuilder sbStr = new StringBuilder();
    for (int i = 0, il = arr.length; i < il; i++) {
      if (i > 0)
        sbStr.append(sep);
      sbStr.append(arr[i]);
    }
    return sbStr.toString();
  }

  public static ObjectMapper getJsonObjectMapper() {
    ObjectMapper jsonObjectMapper = new ObjectMapper();
    // 容忍json中出现未知的列
    jsonObjectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    // 兼容java中的驼峰的字段名命名
    jsonObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.SNAKE_CASE);
    jsonObjectMapper.setTimeZone(TimeZone.getDefault());
    jsonObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    return jsonObjectMapper;
  }

  /**
   * 校验 IDM 用户纬度数据是否合法
   *
   * @param actionType  事件类型
   * @param identityMap 用户纬度数据集合
   * @throws InvalidArgumentException 不合法直接抛出异常
   */
  public static void assertIdentityMap(String actionType, Map<String, String> identityMap)
      throws InvalidArgumentException {
    for (Map.Entry<String, String> entry : identityMap.entrySet()) {
      assertKey(actionType, entry.getKey());
      assertValue(actionType, entry.getValue());
    }
  }

  /**
   * 校验数据属性是否合法
   *
   * @param eventType  事件类型
   * @param properties 数据属性集合
   * @throws InvalidArgumentException 不合法直接抛出异常
   */
  public static void assertProperties(String eventType, Map<String, Object> properties)
      throws InvalidArgumentException {
    if (null == properties) {
      return;
    }
    for (Map.Entry<String, Object> property : properties.entrySet()) {
      if (property.getKey().equals("$is_login_id")) {
        if (!(property.getValue() instanceof Boolean)) {
          throw new InvalidArgumentException("The property value of '$is_login_id' should be Boolean.");
        }
        continue;
      }

      assertKey("property", property.getKey());

      if (!(property.getValue() instanceof Number) && !(property.getValue() instanceof Date)
          && !(property.getValue() instanceof String) && !(property.getValue() instanceof Boolean)
          && !(property.getValue() instanceof List<?>)) {
        String type = property.getValue() == null ? "null" : property.getValue().getClass().toString();
        throw new InvalidArgumentException(String.format(
            "The property '%s' should be a basic type: Number, String, Date, Boolean, List<String>.The current type is %s.",
            property.getKey(), type));
      }

      if (property.getKey().equals(SensorsConst.TIME_SYSTEM_ATTR)
          && !(property.getValue() instanceof Date)) {
        throw new InvalidArgumentException("The property '$time' should be a java.util.Date.");
      }

      // List 类型的属性值，List 元素必须为 String 类型
      if (property.getValue() instanceof List<?> &&
          !eventType.equals(SensorsConst.PROFILE_INCREMENT_ACTION_TYPE)) {
        for (final ListIterator<Object> it = ((List<Object>) property.getValue()).listIterator(); it.hasNext(); ) {
          Object element = it.next();
          if (!(element instanceof String)) {
            throw new InvalidArgumentException(
                String.format("The property '%s' should be a list of String.", property.getKey()));
          }
          if (((String) element).length() > 8192) {
            it.set(((String) element).substring(0, 8192));
          }
        }
      }

      // String 类型的属性值，长度不能超过 8192
      if (property.getValue() instanceof String) {
        String value = (String) property.getValue();
        if (value.length() > 8192) {
          property.setValue(value.substring(0, 8192));
        }
      }

      boolean isFilterKey = property.getKey().equals(SensorsConst.PROJECT_SYSTEM_ATTR) ||
          property.getKey().equals(SensorsConst.TOKEN_SYSTEM_ATTR);

      if (eventType.equals(SensorsConst.PROFILE_INCREMENT_ACTION_TYPE)) {
        if (!isFilterKey) {
          if (!(property.getValue() instanceof Number)) {
            String type = property.getValue() == null ? "null" : property.getValue().getClass().toString();
            throw new InvalidArgumentException(
                String.format("The property value of PROFILE_INCREMENT should be a Number.The current type is %s.",
                    type));
          }
        }
      } else if (eventType.equals(SensorsConst.PROFILE_APPEND_ACTION_TYPE)) {
        if (!isFilterKey && !(property.getValue() instanceof List<?>)) {
          throw new InvalidArgumentException("The property value of PROFILE_APPEND should be a List<String>.");
        }
      }
    }
  }

  public static void assertValue(String type, String value) throws InvalidArgumentException {
    if (value == null || value.length() < 1) {
      throw new InvalidArgumentException(String.format("The %s value is empty or null.", type));
    }
    if (value.length() > 255) {
      throw new InvalidArgumentException(String.format("The %s value %s is too long, max length is 255.", type, value));
    }
  }

  public static void assertKey(String type, String key) throws InvalidArgumentException {
    if (null == key || key.length() < 1) {
      throw new InvalidArgumentException(String.format("The %s key is empty or null.", type));
    }
    if (!(KEY_PATTERN.matcher(key).matches())) {
      throw new InvalidArgumentException(String.format("The %s key '%s' is invalid.", type, key));
    }
  }

  public static Integer getTrackId(Map<String, Object> propertyMap, String message) {
    try {
      if (propertyMap.containsKey(SensorsConst.TRACK_ID)) {
        return Integer.parseInt((String.valueOf(propertyMap.remove(SensorsConst.TRACK_ID))));
      }
      return new Random().nextInt();
    } catch (Exception e) {
      log.error("Failed Get Track ID. {} ", message, e);
      return new Random().nextInt();
    }

  }

  public static String checkUserInfo(Long userId, Map<String, String> identities, String distinctId)
      throws InvalidArgumentException {
    if (identities.isEmpty() && null == userId) {
      throw new InvalidArgumentException("missing user info node(identities/user_id).");
    }
    if (!identities.isEmpty()) {
      Pair<String, Boolean> userPair = checkIdentitiesAndGenerateDistinctId(distinctId, identities);
      return userPair.getKey();
    }
    return null;
  }


  /**
   * IDM3.0 校验 distinctId 和 identities 集合，并生成最终 distinctId
   *
   * @param distinctId 外部传入
   * @param identities 用户维度集合
   * @return 返回最终 distinctId 值和 isLoginId
   */
  public static Pair<String, Boolean> checkIdentitiesAndGenerateDistinctId(String distinctId,
      Map<String, String> identities) throws InvalidArgumentException {
    if (distinctId != null) {
      assertValue("distinct_id", distinctId);
    }
    String tmpId = null;
    //校验 idMap 是否正确，并尝试组装 distinct
    for (Map.Entry<String, String> entry : identities.entrySet()) {
      assertKey(TRACK_ACTION_TYPE, entry.getKey());
      assertValue(TRACK_ACTION_TYPE, entry.getValue());
      if (tmpId == null && distinctId == null) {
        tmpId = String.format("%s+%s", entry.getKey(), entry.getValue());
        if (tmpId.length() > 255) {
          tmpId = null;
        }
      }
    }
    boolean isLoginId = false;
    String id = tmpId;
    //没有主动设置，则使用 SDK 内部逻辑
    if (distinctId == null) {
      //循环完毕,查看一下 map 中是否存在 $identity_login_id
      if (identities.containsKey(SensorsAnalyticsIdentity.LOGIN_ID)) {
        id = identities.get(SensorsAnalyticsIdentity.LOGIN_ID);
        isLoginId = true;
      } else if (tmpId == null) { // 不存在 $identity_login_id，并且 key+value 超长，使用第一个 value
        id = identities.get(identities.keySet().iterator().next());
      } else {// 否则使用 key+value
        id = tmpId;
      }
    } else {
      id = distinctId;
    }
    return Pair.of(id, isLoginId);
  }

  public static void assertFailedData(FailedData failedData) throws InvalidArgumentException {
    if (failedData.getFailedData() == null || failedData.getFailedData().isEmpty()) {
      throw new InvalidArgumentException("Failed Data is null or empty.");
    }
    for (int i = 0; i < failedData.getFailedData().size(); i++) {
      final Map<String, Object> dataMap = failedData.getFailedData().get(i);
      if (verifyMapValueIsBlank(dataMap, "type")) {
        throw new InvalidArgumentException(
            String.format("The index [%d] of failed data list have not [type].", i));
      }
      final String type = String.valueOf(dataMap.get("type"));
      switch (type) {
        case ITEM_SET_ACTION_TYPE:
        case ITEM_DELETE_ACTION_TYPE:
          if (verifyMapValueIsBlank(dataMap, "item_id")) {
            throw new InvalidArgumentException(
                String.format("The index [%d] of failed data list have not [item_id].", i));
          }
          assertValue("item id", dataMap.get("item_id").toString());
          if (verifyMapValueIsBlank(dataMap, "item_type")) {
            throw new InvalidArgumentException(
                String.format("The index [%d] of failed data list have not [item_type].", i));
          }
          assertValue("item type", dataMap.get("item_type").toString());
          break;
        case TRACK_SIGN_UP_ACTION_TYPE:
          if (verifyMapValueIsBlank(dataMap, "original_id")) {
            throw new InvalidArgumentException(
                String.format("The index [%d] of failed data list have not [original_id].", i));
          }
          assertValue("Original Distinct Id", dataMap.get("original_id").toString());
        case BIND_ID_ACTION_TYPE:
        case UNBIND_ID_ACTION_TYPE:
          if (!TRACK_SIGN_UP_ACTION_TYPE.equals(type) && verifyMapValueIsBlank(dataMap, "identities")) {
            throw new InvalidArgumentException(
                String.format("The index [%d] of failed data list have not [identities].", i));
          }
        case TRACK_ACTION_TYPE:
        case PROFILE_SET_ACTION_TYPE:
        case PROFILE_SET_ONCE_ACTION_TYPE:
        case PROFILE_APPEND_ACTION_TYPE:
        case PROFILE_INCREMENT_ACTION_TYPE:
        case PROFILE_UNSET_ACTION_TYPE:
        case PROFILE_DELETE_ACTION_TYPE:
          if (verifyMapValueIsBlank(dataMap, "distinct_id")) {
            throw new InvalidArgumentException(
                String.format("The index [%d] of failed data list have not [distinct_id].", i));
          }
          if (verifyMapValueIsBlank(dataMap, "_track_id") || !(dataMap.get("_track_id") instanceof Integer)) {
            throw new InvalidArgumentException(String.format("The index [%d] of of failed data list is invalid.", i));
          }
          assertValue("Distinct Id", dataMap.get("distinct_id").toString());
          break;
        default:
          throw new InvalidArgumentException(
              String.format("The index [%d] of failed data list can not identify [action_type:%s].", i, type));
      }
      if (dataMap.containsKey("properties")) {
        if (!(dataMap.get("properties") instanceof Map)) {
          throw new InvalidArgumentException(
              String.format("The index [%d] of failed data list can not identify [properties:%s].", i, type));
        }
        assertProperties(type, (Map<String, Object>) dataMap.get("properties"));
      }
      if (!dataMap.containsKey("time")) {
        dataMap.put("time", System.currentTimeMillis());
      }
      //重置 lib,后台获取该数据时能根据 $lib_method 区分通过重发送接口上报到服务端
      dataMap.put("lib", generateLibInfo());
    }
  }

  public static Map<String, String> generateLibInfo() {
    Map<String, String> libProperties = new HashMap<>();
    libProperties.put(LIB_SYSTEM_ATTR, LIB);
    libProperties.put(LIB_VERSION_SYSTEM_ATTR, SDK_VERSION);
    libProperties.put(LIB_METHOD_SYSTEM_ATTR, "code");
    StackTraceElement[] trace = (new Exception()).getStackTrace();
    if (trace.length > 3) {
      StackTraceElement traceElement = trace[3];
      libProperties.put(LIB_DETAIL_SYSTEM_ATTR,
          String.format("%s##%s##%s##%s", traceElement.getClassName(), traceElement.getMethodName(),
              traceElement.getFileName(), traceElement.getLineNumber()));
    }
    return libProperties;
  }

  public static List<Map<String, Object>> deepCopy(List<Map<String, Object>> source) {
    if (source == null) {
      return null;
    }
    List<Map<String, Object>> newList = new ArrayList<>(source.size());
    for (Map<String, Object> objectMap : source) {
      Map<String, Object> newMap = new HashMap<>(objectMap.size());
      newMap.putAll(objectMap);
      newList.add(newMap);
    }
    return newList;
  }

  private static boolean verifyMapValueIsBlank(Map<String, Object> map, String key) {
    return map == null || !map.containsKey(key) || map.get(key) == null;
  }

  public static void assertSchema(String schema) throws InvalidArgumentException {
    if (null == schema || "".equals(schema)) {
      throw new InvalidArgumentException("The schema can not set null or empty.");
    }
    if (!KEY_PATTERN.matcher(schema).matches()) {
      throw new InvalidArgumentException(String.format("Thr schema '%s' is invalid.", schema));
    }
  }

  public static void assertSchemaProperties(Map<String, Object> properties, String actionType)
      throws InvalidArgumentException {
    if (properties.isEmpty()) {
      return;
    }
    for (Map.Entry<String, Object> entry : properties.entrySet()) {
      assertKey("property", entry.getKey());
      Object value = entry.getValue();
      if (SensorsConst.TIME_SYSTEM_ATTR.equals(entry.getKey()) && !(value instanceof Date)) {
        throw new InvalidArgumentException("The property '$time' should be a java.util.Date.");
      }
      switch (SensorsDataTypeEnum.getDataType(value)) {
        case NUMBER:
        case BOOLEAN:
        case DATE:
          break;
        case LIST:
          List<Object> valueList = (List<Object>) value;
          ListIterator<Object> listIterator = valueList.listIterator();
          while (listIterator.hasNext()) {
            Object next = listIterator.next();
            if (!(next instanceof String)) {
              throw new InvalidArgumentException(
                  String.format("The property '%s' should be a list of String.", entry.getKey()));
            }
            if (((String) next).length() > 8192) {
              listIterator.set(((String) next).substring(0, 8192));
            }
          }
          break;
        case STRING:
          if (((String) value).length() > 8192) {
            entry.setValue(((String) value).substring(0, 8192));
          }
          break;
        default:
          throw new InvalidArgumentException(String.format(
              "The property '%s' should be a basic type: Number, String, Date, Boolean, List<String>.The current type is %s.",
              entry.getKey(), value.getClass().getName()));
      }
      //处理 profileIncrement
      if (PROFILE_INCREMENT_ACTION_TYPE.equals(actionType)) {
        if ((!SensorsConst.PROJECT_SYSTEM_ATTR.equals(entry.getKey())
            && !SensorsConst.TOKEN_SYSTEM_ATTR.equals(entry.getKey())) && !(value instanceof Number)) {
          throw new InvalidArgumentException(
              String.format("The property value of PROFILE_INCREMENT should be a Number.The current type is %s.",
                  value.getClass().getName()));
        }
      }
      //处理 profileAppend
      if (PROFILE_APPEND_ACTION_TYPE.equals(actionType)) {
        if ((!SensorsConst.PROJECT_SYSTEM_ATTR.equals(entry.getKey())
            && !SensorsConst.TOKEN_SYSTEM_ATTR.equals(entry.getKey())) && !(value instanceof List<?>)) {
          throw new InvalidArgumentException(
              String.format("The property value of PROFILE_APPEND should be a List<String>.The current type is %s.",
                  value.getClass().getName()));
        }
      }
      //处理 profile_unset
      if (PROFILE_UNSET_ACTION_TYPE.equals(actionType)) {
        if (!(value instanceof Boolean) || !Boolean.parseBoolean(value.toString())) {
          throw new InvalidArgumentException("The property value of " + entry.getKey() + " should be true.");
        }
      }
    }
  }


  public static String toString(Map<String, String> identities) {
    if (identities == null) {
      return null;
    }
    StringBuilder res = new StringBuilder("");
    for (Map.Entry<String, String> entry : identities.entrySet()) {
      res.append("{").append(entry.getKey()).append(":").append(entry.getValue()).append("},");
    }
    return res.toString();
  }
}