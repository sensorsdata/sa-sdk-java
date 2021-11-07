package com.sensorsdata.analytics.javasdk.util;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.ListIterator;
import java.util.Map;
import java.util.TimeZone;
import java.util.regex.Pattern;

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
    jsonObjectMapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);
    jsonObjectMapper.setTimeZone(TimeZone.getDefault());
    jsonObjectMapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS"));
    return jsonObjectMapper;
  }

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

      if (property.getKey().equals(SensorsConst.TINE_SYSTEM_ATTR)
          && !(property.getValue() instanceof Date)) {
        throw new InvalidArgumentException("The property '$time' should be a java.util.Date.");
      }

      // List 类型的属性值，List 元素必须为 String 类型
      if (property.getValue() instanceof List<?> && !eventType.equals(
          SensorsConst.PROFILE_INCREMENT_ACTION_TYPE)) {
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
}