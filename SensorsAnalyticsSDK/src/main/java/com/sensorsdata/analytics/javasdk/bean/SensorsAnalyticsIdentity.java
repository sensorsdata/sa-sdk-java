package com.sensorsdata.analytics.javasdk.bean;

import java.util.LinkedHashMap;
import java.util.Map;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter(AccessLevel.PROTECTED)
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class SensorsAnalyticsIdentity {
    // 用户登录 ID
    public static final String LOGIN_ID = "$identity_login_id";
    // 手机号
    public static final String MOBILE = "$identity_mobile";
    // 邮箱
    public static final String EMAIL = "$identity_email";
    /** 用户纬度标识集合 */
    protected Map<String, String> identityMap;

    public static Builder builder() {
        return new Builder();
    }

    @NoArgsConstructor(access = AccessLevel.PRIVATE)
    public static class Builder {
        private final Map<String, String> idMap = new LinkedHashMap<>();

        public SensorsAnalyticsIdentity.Builder identityMap(Map<String, String> identityMap) {
            if (identityMap != null) {
                this.idMap.putAll(identityMap);
            }
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
