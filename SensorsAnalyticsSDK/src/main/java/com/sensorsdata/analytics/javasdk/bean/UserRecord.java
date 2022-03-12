package com.sensorsdata.analytics.javasdk.bean;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 用户信息实体对象
 *
 * @author fz
 * @version 1.0.0
 * @since 2021/05/24 15:52
 */
public class UserRecord implements Serializable {

    private static final long serialVersionUID = -6661580072748171679L;

    private final Map<String, Object> propertyMap;

    private final String distinctId;

    private final Boolean isLoginId;

    private UserRecord(Map<String, Object> propertyMap, String distinctId, Boolean isLoginId) {
        this.propertyMap = propertyMap;
        this.distinctId = distinctId;
        this.isLoginId = isLoginId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public String getDistinctId() {
        return distinctId;
    }

    public Boolean getIsLoginId() {
        return isLoginId;
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public static class Builder {
        private final Map<String, Object> propertyMap = new HashMap<String, Object>();
        private String distinctId;
        private Boolean isLoginId;

        private Builder() {
        }

        public UserRecord build() throws InvalidArgumentException {
            if (distinctId == null) {
                throw new InvalidArgumentException("The distinctId is empty.");
            }
            if (isLoginId == null) {
                throw new InvalidArgumentException("The isLoginId is empty.");
            }
            return new UserRecord(propertyMap, distinctId, isLoginId);
        }

        public UserRecord.Builder setDistinctId(String distinctId) {
            this.distinctId = distinctId;
            return this;
        }

        public UserRecord.Builder isLoginId(Boolean loginId) {
            this.isLoginId = loginId;
            return this;
        }

        public UserRecord.Builder addProperties(Map<String, Object> properties) {
            if (properties != null) {
                propertyMap.putAll(properties);
            }
            return this;
        }

        public UserRecord.Builder addProperty(String key, String property) {
            addPropertyObject(key, property);
            return this;
        }

        public UserRecord.Builder addProperty(String key, boolean property) {
            addPropertyObject(key, property);
            return this;
        }

        public UserRecord.Builder addProperty(String key, Number property) {
            addPropertyObject(key, property);
            return this;
        }

        public UserRecord.Builder addProperty(String key, Date property) {
            addPropertyObject(key, property);
            return this;
        }

        public UserRecord.Builder addProperty(String key, List<String> property) {
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
