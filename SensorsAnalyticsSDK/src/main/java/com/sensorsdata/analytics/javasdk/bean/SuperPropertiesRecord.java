package com.sensorsdata.analytics.javasdk.bean;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 公共属性信息实体
 *
 * @author fz
 * @version 1.0.0
 * @since 2021/05/26 11:09
 */
public class SuperPropertiesRecord implements Serializable {

    private static final long serialVersionUID = 6526668600748384833L;

    private final Map<String, Object> propertyMap;

    private SuperPropertiesRecord(Map<String, Object> propertyMap) {
        this.propertyMap = propertyMap;
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private Map<String, Object> propertyMap = new HashMap<String, Object>();

        private Builder() {
        }

        public SuperPropertiesRecord build() throws InvalidArgumentException {
            if (propertyMap.size() == 0) {
                throw new InvalidArgumentException("The propertyMap is empty.");
            }
            return new SuperPropertiesRecord(propertyMap);
        }


        public SuperPropertiesRecord.Builder addProperties(Map<String, Object> properties) {
            if (properties != null) {
                propertyMap.putAll(properties);
            }
            return this;
        }

        public SuperPropertiesRecord.Builder addProperty(String key, String property) {
            addPropertyObject(key, property);
            return this;
        }

        public SuperPropertiesRecord.Builder addProperty(String key, boolean property) {
            addPropertyObject(key, property);
            return this;
        }

        public SuperPropertiesRecord.Builder addProperty(String key, Number property) {
            addPropertyObject(key, property);
            return this;
        }

        public SuperPropertiesRecord.Builder addProperty(String key, Date property) {
            addPropertyObject(key, property);
            return this;
        }

        public SuperPropertiesRecord.Builder addProperty(String key, List<String> property) {
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
