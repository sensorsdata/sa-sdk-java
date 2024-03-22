package com.sensorsdata.analytics.javasdk.bean.schema;

import com.sensorsdata.analytics.javasdk.common.Pair;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.Getter;
import lombok.NonNull;

/**
 * 明细数据入参
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2023/03/21 11:19
 */
@Getter
public class DetailSchema {
    /**
     * detail 对应的 schema
     *
     * <p>必传参数，若不传入，则抛出 InvalidArgumentException 异常；
     */
    private String schema;
    /**
     * detail 对应的 id
     *
     * <p>必传参数，若不传入，则抛出 InvalidArgumentException 异常；
     */
    private String detailId;
    /**
     * detail 的自定义属性集合
     *
     * <p>非必传参数
     */
    private Map<String, Object> properties;
    /**
     * 若传入该参数，该 detail 则被视为该用户实体的明细
     *
     * <p>非必传参数,跟 itemId 节点互斥，若同时传入，则会抛出 InvalidArgumentException 异常；
     */
    private Map<String, String> identities;
    /**
     * 只有传入 identities 节点信息，再设置该节点才会生效，否则设置该值无效；
     *
     * <p>非必传参数,不传此值，会取 identities 节点集合里第一个节点的用户信息作为 distinct_id
     */
    private String distinctId;
    /**
     * 设置该参数，则该 detail 被视为 item 实体的明细
     *
     * <p>非必传参数,跟 identities 节点互斥。若同时传入，则会抛出 InvalidArgumentException 异常；
     */
    private Pair<String, String> itemPair;

    private Integer trackId;

    protected DetailSchema(
            String schema,
            String detailId,
            Map<String, Object> properties,
            Map<String, String> identities,
            String distinctId,
            Pair<String, String> itemPair,
            Integer trackId) {
        this.schema = schema;
        this.detailId = detailId;
        this.properties = properties;
        this.identities = identities;
        this.distinctId = distinctId;
        this.itemPair = itemPair;
        this.trackId = trackId;
    }

    public static Builder init() {
        return new Builder();
    }

    public static class Builder {
        private String schema;
        private String detailId;
        private Map<String, Object> properties = new HashMap<>();
        private Map<String, String> identities = new LinkedHashMap<>();
        private String distinctId;
        private Pair<String, String> itemPair;
        private Integer trackId;

        public DetailSchema start() throws InvalidArgumentException {
            SensorsAnalyticsUtil.assertSchema(schema);
            SensorsAnalyticsUtil.assertValue("detail_id", detailId);
            if (!identities.isEmpty() && null != itemPair) {
                throw new InvalidArgumentException(
                        "detail schema cannot both set identities and itemPair.");
            }
            this.trackId =
                    SensorsAnalyticsUtil.getTrackId(
                            properties,
                            String.format(
                                    "detail generate trackId error.[distinct_id=%s,detail_id=%s,schema=%s]",
                                    distinctId, detailId, schema));
            if (!identities.isEmpty()) {
                this.distinctId = SensorsAnalyticsUtil.checkUserInfo(null, identities, distinctId);
            }
            SensorsAnalyticsUtil.assertSchemaProperties(properties, null);
            return new DetailSchema(
                    schema, detailId, properties, identities, distinctId, itemPair, trackId);
        }

        public Builder setSchema(@NonNull String schema) {
            this.schema = schema;
            return this;
        }

        public Builder setDetailId(@NonNull String detailId) {
            this.detailId = detailId;
            return this;
        }

        public Builder setItemPair(@NonNull String key, @NonNull String value) {
            this.itemPair = Pair.of(key, value);
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

        public Builder identityMap(@NonNull Map<String, String> identities) {
            this.identities.putAll(identities);
            return this;
        }

        public Builder addIdentityProperty(@NonNull String key, @NonNull String value) {
            this.identities.put(key, value);
            return this;
        }

        public Builder setDistinctId(@NonNull String distinctId) {
            this.distinctId = distinctId;
            return this;
        }

        private void addPropertyObject(@NonNull String key, @NonNull Object property) {
            this.properties.put(key, property);
        }
    }
}
