package com.sensorsdata.analytics.javasdk.bean;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 事件表信息实体对象
 */
public class EventRecord implements Serializable {
    private static final long serialVersionUID = -2327319579147636283L;

    private final Map<String, Object> propertyMap;

    private final String eventName;

    private final String distinctId;

    private final Boolean isLoginId;

    private EventRecord(String eventName, String distinctId, Boolean isLoginId, Map<String, Object> propertyMap) {
        this.eventName = eventName;
        this.distinctId = distinctId;
        this.isLoginId = isLoginId;
        this.propertyMap = propertyMap;
    }

    @Override
    public String toString() {
        return "EventRecord{" +
                   "propertyMap=" + propertyMap +
                   ", eventName='" + eventName + '\'' +
                   ", distinctId='" + distinctId + '\'' +
                   ", isLoginId='" + isLoginId + '\'' +
                   '}';
    }

    public static Builder builder() {
        return new Builder();
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public String getEventName() {
        return eventName;
    }

    public String getDistinctId() {
        return distinctId;
    }

    public Boolean getIsLoginId() {
        return isLoginId;
    }

    public static class Builder {
        private Map<String, Object> propertyMap = new HashMap<String, Object>();
        private String eventName;
        private String distinctId;
        private Boolean isLoginId;

        private Builder() {
        }

        public EventRecord build() throws InvalidArgumentException {
            if (eventName == null) {
                throw new InvalidArgumentException("The eventName is empty.");
            }
            if (distinctId == null) {
                throw new InvalidArgumentException("The distinctId is empty.");
            }
            if (isLoginId == null) {
                throw new InvalidArgumentException("The isLoginId is empty.");
            }
            return new EventRecord(eventName, distinctId, isLoginId, propertyMap);
        }

        public EventRecord.Builder setEventName(String eventName) {
            this.eventName = eventName;
            return this;
        }

        public EventRecord.Builder setDistinctId(String distinctId) {
            this.distinctId = distinctId;
            return this;
        }

        public EventRecord.Builder isLoginId(Boolean loginId) {
            this.isLoginId = loginId;
            return this;
        }

        public EventRecord.Builder addProperties(Map<String, Object> properties) {
            if (properties != null) {
                propertyMap.putAll(properties);
            }
            return this;
        }

        public EventRecord.Builder addProperty(String key, String property) {
            addPropertyObject(key, property);
            return this;
        }

        public EventRecord.Builder addProperty(String key, boolean property) {
            addPropertyObject(key, property);
            return this;
        }

        public EventRecord.Builder addProperty(String key, Number property) {
            addPropertyObject(key, property);
            return this;
        }

        public EventRecord.Builder addProperty(String key, Date property) {
            addPropertyObject(key, property);
            return this;
        }

        public EventRecord.Builder addProperty(String key, List<String> property) {
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