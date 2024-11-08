package com.sensorsdata.analytics.javasdk.common;

import java.util.Date;
import java.util.List;

/**
 * 神策数据类型枚举
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/15 17:34
 */
public enum SensorsDataTypeEnum {
    NUMBER,
    STRING,
    BOOLEAN,
    DATE,
    LIST,
    UNKNOWN;

    public static SensorsDataTypeEnum getDataType(Object value) {
        if (value instanceof Number) {
            return SensorsDataTypeEnum.NUMBER;
        } else if (value instanceof String) {
            return SensorsDataTypeEnum.STRING;
        } else if (value instanceof Boolean) {
            return SensorsDataTypeEnum.BOOLEAN;
        } else if (value instanceof Date) {
            return SensorsDataTypeEnum.DATE;
        } else if (value instanceof List<?>) {
            return SensorsDataTypeEnum.LIST;
        } else {
            return SensorsDataTypeEnum.UNKNOWN;
        }
    }
}
