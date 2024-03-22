package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.BIND_ID_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_SIGN_UP_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.UNBIND_ID_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.bean.schema.DetailSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.common.Pair;
import com.sensorsdata.analytics.javasdk.common.SchemaTypeEnum;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;

/**
 * 神策多实体数据
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/14 14:44
 */
@Slf4j
class SensorsSchemaData extends SensorsData {

    private String version = SensorsConst.PROTOCOL_VERSION;

    private String schema;

    private Long userId;

    private String detailId;

    private Pair<String, String> itemEventPair;

    private SchemaTypeEnum schemaTypeEnum;

    /** 构建 userEventSchema 数据，actionType:track/bind/unbind */
    protected SensorsSchemaData(UserEventSchema userEventSchema, String actionType) {
        super(
                userEventSchema.getDistinctId(),
                null,
                userEventSchema.getIdentityMap(),
                actionType,
                userEventSchema.getEventName(),
                userEventSchema.getPropertyMap(),
                null,
                null,
                userEventSchema.getTrackId());
        this.schema = SensorsConst.USER_EVENT_SCHEMA;
        this.userId = userEventSchema.getUserId();
        this.schemaTypeEnum = SchemaTypeEnum.USER_EVENT;
    }

    protected SensorsSchemaData(ItemSchema itemSchema, String actionType) {
        super(
                itemSchema.getTrackId(),
                null,
                null,
                itemSchema.getItemId(),
                actionType,
                null,
                itemSchema.getProperties());
        this.schema = itemSchema.getSchema();
        this.schemaTypeEnum = SchemaTypeEnum.ITEM;
    }

    protected SensorsSchemaData(ItemEventSchema itemEventSchema, String actionType) {
        super(
                itemEventSchema.getTrackId(),
                null,
                null,
                null,
                actionType,
                itemEventSchema.getEventName(),
                itemEventSchema.getProperties());
        this.schema = itemEventSchema.getSchema();
        this.itemEventPair = itemEventSchema.getItemPair();
        this.schemaTypeEnum = SchemaTypeEnum.ITEM_EVENT;
    }

    protected SensorsSchemaData(UserSchema userSchema, String actionType) {
        super(
                userSchema.getDistinctId(),
                actionType,
                userSchema.getIdentityMap(),
                userSchema.getPropertyMap(),
                userSchema.getTrackId());
        this.schema = SensorsConst.USER_SCHEMA;
        this.userId = userSchema.getUserId();
        this.schemaTypeEnum = SchemaTypeEnum.USER;
    }

    protected SensorsSchemaData(DetailSchema detailSchema, String actionType) {
        super(
                detailSchema.getTrackId(),
                detailSchema.getDistinctId(),
                detailSchema.getIdentities(),
                null,
                actionType,
                null,
                detailSchema.getProperties());
        this.schema = detailSchema.getSchema();
        this.itemEventPair = detailSchema.getItemPair();
        this.detailId = detailSchema.getDetailId();
        this.schemaTypeEnum = SchemaTypeEnum.DETAIL;
    }

    public Map<String, Object> generateData() {
        Map<String, Object> data = new HashMap<>();
        data.put("_track_id", getTrackId());
        data.put("version", version);
        data.put("type", getType());
        data.put("schema", schema);
        data.put("lib", getLib());
        data.put("time", getTime().getTime());
        if (getProject() != null && !"".equals(getProject())) {
            data.put("project", getProject());
        }
        if (getToken() != null && !"".equals(getToken())) {
            data.put("token", getToken());
        }
        switch (schemaTypeEnum) {
            case ITEM:
                data.put("id", getItemId());
                break;
            case ITEM_EVENT:
                getProperties().put(itemEventPair.getKey(), itemEventPair.getValue());
                addTimeFree(data);
                data.put("event", getEvent());
                break;
            case USER:
                checkUserIdAndAddUser(data, "id");
                break;
            case USER_EVENT:
                addTimeFree(data);
                data.put("event", getEvent());
                checkUserIdAndAddUser(getProperties(), "user_id");
                break;
            case USER_ITEM:
                data.put("id", getItemId());
                checkUserIdAndAddUser(getProperties(), "user_id");
                break;
            case DETAIL:
                data.put("id", detailId);
                if (itemEventPair != null) {
                    getProperties().put(itemEventPair.getKey(), itemEventPair.getValue());
                }
                if (!getIdentities().isEmpty()) {
                    checkUserIdAndAddUser(getProperties(), "user_id");
                }
            default:
                break;
        }
        data.put("properties", getProperties());
        return data;
    }

    public boolean isEventSchemaData() {
        return SchemaTypeEnum.ITEM_EVENT.equals(schemaTypeEnum)
                || SchemaTypeEnum.USER_EVENT.equals(schemaTypeEnum);
    }

    public String getVersion() {
        return version;
    }

    public String getSchema() {
        return schema;
    }

    public Long getUserId() {
        return userId;
    }

    public SchemaTypeEnum getSchemaTypeEnum() {
        return schemaTypeEnum;
    }

    private void addTimeFree(Map<String, Object> data) {
        if (isTimeFree()
                && (TRACK_ACTION_TYPE.equals(getType())
                        || TRACK_SIGN_UP_ACTION_TYPE.equals(getType())
                        || BIND_ID_ACTION_TYPE.equals(getType())
                        || UNBIND_ID_ACTION_TYPE.equals(getType()))) {
            data.put("time_free", true);
        }
    }

    private void checkUserIdAndAddUser(Map<String, Object> data, String key) {
        if (null != getUserId()) {
            data.put(key, getUserId());
        } else if (null != getIdentities() && !getIdentities().isEmpty()) {
            data.put("identities", getIdentities());
        }
        if (null != getDistinctId()) {
            data.put("distinct_id", getDistinctId());
        }
    }
}
