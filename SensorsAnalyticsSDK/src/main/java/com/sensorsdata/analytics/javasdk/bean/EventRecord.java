package com.sensorsdata.analytics.javasdk.bean;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

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

    private final Integer trackId;

    private final String originalId;

    private EventRecord(String eventName, String distinctId, Boolean isLoginId, Map<String, Object> propertyMap,
            Integer trackId, String originalId) {
        this.eventName = eventName;
        this.distinctId = distinctId;
        this.isLoginId = isLoginId;
        if (isLoginId) {
            propertyMap.put(SensorsConst.LOGIN_SYSTEM_ATTR, true);
        }
        this.propertyMap = propertyMap;
        this.trackId = trackId;
        this.originalId = originalId;
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

    public String getOriginalId() {
        return originalId;
    }

    public Integer getTrackId() {return trackId; }

    public static class Builder {
        private final Map<String, Object> propertyMap = new HashMap<>();
        private String eventName;
        private String distinctId;
        private Boolean isLoginId;
        private Integer trackId;
        private String originalId;

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
            SensorsAnalyticsUtil.assertKey("event_name",eventName);
            SensorsAnalyticsUtil.assertValue("distinct_id", distinctId);
            String message = String.format("[distinct_id=%s,event_name=%s,is_login_id=%s]",distinctId,eventName,isLoginId);
            trackId = SensorsAnalyticsUtil.getTrackId(propertyMap, message);
            return new EventRecord(eventName, distinctId, isLoginId, propertyMap,trackId, originalId);
        }

        public EventRecord.Builder setOriginalId(String originalId) {
            this.originalId = originalId;
            return this;
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