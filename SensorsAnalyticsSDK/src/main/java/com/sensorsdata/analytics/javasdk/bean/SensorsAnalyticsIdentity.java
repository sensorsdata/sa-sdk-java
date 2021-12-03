package com.sensorsdata.analytics.javasdk.bean;

import lombok.*;

import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
@AllArgsConstructor(access = AccessLevel.PRIVATE)
public class SensorsAnalyticsIdentity {
    // 用户登录 ID
    public static final String LOGIN_ID = "$identity_login_id";
    // 手机号
    public static final String MOBILE = "$identity_mobile";
    // 邮箱
    public static final String EMAIL = "$identity_email";

    private Map<String, String> identityMap;

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private Map<String, String> idMap = new HashMap<String, String>();

        public SensorsAnalyticsIdentity.Builder identityMap(Map<String, String> identityMap) {
            this.idMap.putAll(identityMap);
            return this;
        }

        public SensorsAnalyticsIdentity.Builder addIdentityProperty(String key, String value) {
            this.idMap.put(key, value);
            return this;
        }

        public SensorsAnalyticsIdentity build() {
            return new SensorsAnalyticsIdentity(idMap);
        }
    }
}
