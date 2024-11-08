package com.sensorsdata.analytics.javasdk.bean.schema;

import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;

/**
 * identitySchema
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/07/25 17:30
 */
@Getter
public class IdentitySchema {

    private Map<String, String> idMap;

    private Map<String, Object> properties;

    protected IdentitySchema(Map<String, String> idMap, Map<String, Object> properties) {
        this.idMap = idMap;
        this.properties = properties;
    }

    public static IdentitySchema.Builder init() {
        return new IdentitySchema.Builder();
    }

    public static class Builder {

        private Map<String, String> idMap = new LinkedHashMap<>();

        private Map<String, Object> properties = new HashMap<>();

        public Builder identityMap(Map<String, String> identityMap) {
            if (identityMap != null) {
                this.idMap.putAll(identityMap);
            }
            return this;
        }

        public Builder addIdentityProperty(String key, String value) {
            this.idMap.put(key, value);
            return this;
        }

        public Builder addProperties(@NonNull Map<String, Object> properties) {
            this.properties.putAll(properties);
            return this;
        }

        public Builder addProperty(@NonNull String key, @NonNull String property) {
            addPropertyObject(key, property);
            return this;
        }

        public Builder addProperty(@NonNull String key, boolean property) {
            addPropertyObject(key, property);
            return this;
        }

        public Builder addProperty(@NonNull String key, @NonNull Number property) {
            addPropertyObject(key, property);
            return this;
        }

        public Builder addProperty(@NonNull String key, @NonNull Date property) {
            addPropertyObject(key, property);
            return this;
        }

        public Builder addProperty(@NonNull String key, @NonNull List<String> property) {
            addPropertyObject(key, property);
            return this;
        }

        private void addPropertyObject(@NonNull String key, @NonNull Object property) {
            this.properties.put(key, property);
        }

        public IdentitySchema build() {
            return new IdentitySchema(idMap, properties);
        }
    }
}
