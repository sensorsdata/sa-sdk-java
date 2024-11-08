package com.sensorsdata.analytics.javasdk.bean;

import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_ID;
import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_TYPE;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 纬度表信息实体对象
 *
 * @author fz
 * @version 1.0.0
 * @since 2021/05/24 17:25
 */
public class ItemRecord implements Serializable {

    private static final long serialVersionUID = -3294038187552297656L;

    private final Map<String, Object> propertyMap;

    private final String itemId;

    private final String itemType;

    private final Integer trackId;

    private ItemRecord(
            Map<String, Object> propertyMap, String itemId, String itemType, Integer trackId) {
        this.propertyMap = propertyMap;
        this.itemId = itemId;
        this.itemType = itemType;
        this.trackId = trackId;
    }

    public Map<String, Object> getPropertyMap() {
        return propertyMap;
    }

    public String getItemId() {
        return itemId;
    }

    public String getItemType() {
        return itemType;
    }

    public Integer getTrackId() {
        return trackId;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Map<String, Object> propertyMap = new HashMap<>();

        private String itemId;

        private String itemType;

        private Integer trackId;

        private Builder() {}

        public ItemRecord build() throws InvalidArgumentException {
            if (null == itemId) {
                throw new InvalidArgumentException("The itemId is empty.");
            }
            if (null == itemType) {
                throw new InvalidArgumentException("The itemType is empty.");
            }
            SensorsAnalyticsUtil.assertKey(ITEM_TYPE, itemType);
            SensorsAnalyticsUtil.assertValue(ITEM_ID, itemId);
            this.trackId = null;
            return new ItemRecord(propertyMap, itemId, itemType, trackId);
        }

        public ItemRecord.Builder setItemId(String itemId) {
            this.itemId = itemId;
            return this;
        }

        public ItemRecord.Builder setItemType(String itemType) {
            this.itemType = itemType;
            return this;
        }

        public ItemRecord.Builder addProperties(Map<String, Object> properties) {
            if (properties != null) {
                propertyMap.putAll(properties);
            }
            return this;
        }

        public ItemRecord.Builder addProperty(String key, String property) {
            addPropertyObject(key, property);
            return this;
        }

        public ItemRecord.Builder addProperty(String key, boolean property) {
            addPropertyObject(key, property);
            return this;
        }

        public ItemRecord.Builder addProperty(String key, Number property) {
            addPropertyObject(key, property);
            return this;
        }

        public ItemRecord.Builder addProperty(String key, Date property) {
            addPropertyObject(key, property);
            return this;
        }

        public ItemRecord.Builder addProperty(String key, List<String> property) {
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
