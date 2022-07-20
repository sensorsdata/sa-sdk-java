package com.sensorsdata.analytics.javasdk;

import static com.sensorsdata.analytics.javasdk.SensorsConst.BIND_ID;
import static com.sensorsdata.analytics.javasdk.SensorsConst.BIND_ID_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_DELETE_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.ITEM_SET_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_APPEND_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_DELETE_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_INCREMENT_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_SET_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_SET_ONCE_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROFILE_UNSET_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.PROJECT_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.SIGN_UP_SYSTEM_ATTR;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.TRACK_SIGN_UP_ACTION_TYPE;
import static com.sensorsdata.analytics.javasdk.SensorsConst.UNBIND_ID;
import static com.sensorsdata.analytics.javasdk.SensorsConst.UNBIND_ID_ACTION_TYPE;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.SuperPropertiesRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.consumer.Consumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Sensors Analytics SDK
 */
@Slf4j
public class SensorsAnalytics implements ISensorsAnalytics {

    private final SensorsAnalyticsWorker worker;

    private static final String DISTINCT_ID = "distinct_id";
    private static final String EVENT_NAME = "event name";
    private static final String ORIGINAL_DISTINCT_ID = "Original Distinct Id";

    public SensorsAnalytics(final Consumer consumer) {
        worker = new SensorsAnalyticsWorker(consumer);
    }

    @Override
    public void setEnableTimeFree(@NonNull boolean enableTimeFree) {
        worker.setEnableTimeFree(enableTimeFree);
    }

    @Override
    public void registerSuperProperties(@NonNull SuperPropertiesRecord propertiesRecord) {
        worker.setSuperProperties(propertiesRecord.getPropertyMap());
    }

    @Override
    public void registerSuperProperties(@NonNull Map<String, Object> superPropertiesMap) {
        worker.setSuperProperties(superPropertiesMap);
    }

    @Override
    public void clearSuperProperties() {
        worker.clearSuperProperties();
    }

    @Override
    public void track(@NonNull EventRecord eventRecord) throws InvalidArgumentException {
        addEvent(eventRecord.getDistinctId(), eventRecord.getIsLoginId(), null, TRACK_ACTION_TYPE,
            eventRecord.getEventName(), eventRecord.getPropertyMap());
    }

    @Override
    public void profileSet(@NonNull UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, PROFILE_SET_ACTION_TYPE);
    }

    @Override
    public void profileSetOnce(@NonNull UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, PROFILE_SET_ONCE_ACTION_TYPE);
    }

    @Override
    public void profileAppend(@NonNull UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, PROFILE_APPEND_ACTION_TYPE);
    }

    @Override
    public void profileIncrement(@NonNull UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, PROFILE_INCREMENT_ACTION_TYPE);
    }

    @Override
    public void profileUnset(@NonNull UserRecord userRecord) throws InvalidArgumentException {
        if (userRecord.getPropertyMap() == null) {
            return;
        }
        for (Map.Entry<String, Object> property : userRecord.getPropertyMap().entrySet()) {
            if (!PROJECT_SYSTEM_ATTR.equals(property.getKey())) {
                if (property.getValue() instanceof Boolean) {
                    boolean value = (Boolean) property.getValue();
                    if (value) {
                        continue;
                    }
                }
                throw new InvalidArgumentException("The property value of " + property.getKey() + " should be true.");
            }
        }
        dealProfile(userRecord, PROFILE_UNSET_ACTION_TYPE);
    }

    @Override
    public void profileDelete(@NonNull UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, PROFILE_DELETE_ACTION_TYPE);
    }

    @Override
    public void itemSet(@NonNull ItemRecord itemRecord) throws InvalidArgumentException {
        addItem(itemRecord.getItemType(), itemRecord.getItemId(), ITEM_SET_ACTION_TYPE,
            itemRecord.getPropertyMap());
    }

    @Override
    public void itemDelete(@NonNull ItemRecord itemRecord) throws InvalidArgumentException {
        addItem(itemRecord.getItemType(), itemRecord.getItemId(), ITEM_DELETE_ACTION_TYPE,
            itemRecord.getPropertyMap());
    }

    @Override
    public void track(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String eventName)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, TRACK_ACTION_TYPE, eventName, null);
    }

    @Override
    public void track(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String eventName,
        Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, TRACK_ACTION_TYPE, eventName, properties);
    }

    @Override
    public void trackSignUp(@NonNull String loginId, @NonNull String anonymousId) throws InvalidArgumentException {
        addEvent(loginId, false, anonymousId, TRACK_SIGN_UP_ACTION_TYPE, SIGN_UP_SYSTEM_ATTR, null);
    }

    @Override
    public void trackSignUp(@NonNull String loginId, @NonNull String anonymousId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(loginId, false, anonymousId, TRACK_SIGN_UP_ACTION_TYPE, SIGN_UP_SYSTEM_ATTR, properties);
    }

    @Override
    public void profileSet(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        dealProfile(distinctId, isLoginId, properties, PROFILE_SET_ACTION_TYPE);
    }

    @Override
    public void profileSet(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        dealProfile(distinctId, isLoginId, properties, PROFILE_SET_ACTION_TYPE);
    }

    @Override
    public void profileSetOnce(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        dealProfile(distinctId, isLoginId, properties, PROFILE_SET_ONCE_ACTION_TYPE);
    }

    @Override
    public void profileSetOnce(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        dealProfile(distinctId, isLoginId, properties, PROFILE_SET_ONCE_ACTION_TYPE);
    }

    @Override
    public void profileIncrement(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        dealProfile(distinctId, isLoginId, properties, PROFILE_INCREMENT_ACTION_TYPE);
    }

    @Override
    public void profileIncrement(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull long value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        dealProfile(distinctId, isLoginId, properties, PROFILE_INCREMENT_ACTION_TYPE);
    }

    @Override
    public void profileAppend(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        dealProfile(distinctId, isLoginId, properties, PROFILE_APPEND_ACTION_TYPE);
    }

    @Override
    public void profileAppend(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull String value) throws InvalidArgumentException {
        List<String> values = new ArrayList<>();
        values.add(value);
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, values);
        dealProfile(distinctId, isLoginId, properties, PROFILE_APPEND_ACTION_TYPE);
    }

    @Override
    public void profileUnset(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, true);
        dealProfile(distinctId, isLoginId, properties, PROFILE_UNSET_ACTION_TYPE);
    }

    @Override
    public void profileUnset(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            if (!PROJECT_SYSTEM_ATTR.equals(property.getKey())) {
                if (property.getValue() instanceof Boolean) {
                    boolean value = (Boolean) property.getValue();
                    if (value) {
                        continue;
                    }
                }
                throw new InvalidArgumentException("The property value of " + property.getKey() + " should be true.");
            }
        }
        dealProfile(distinctId, isLoginId, properties, PROFILE_UNSET_ACTION_TYPE);
    }

    @Override
    public void profileDelete(@NonNull String distinctId, @NonNull boolean isLoginId) throws InvalidArgumentException {
        dealProfile(distinctId, isLoginId, null, PROFILE_DELETE_ACTION_TYPE);
    }

    @Override
    public void itemSet(@NonNull String itemType, @NonNull String itemId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addItem(itemType, itemId, ITEM_SET_ACTION_TYPE, properties);
    }

    @Override
    public void itemDelete(@NonNull String itemType, @NonNull String itemId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addItem(itemType, itemId, ITEM_DELETE_ACTION_TYPE, properties);
    }

    @Override
    public void bind(@NonNull SensorsAnalyticsIdentity... identities) throws InvalidArgumentException {
        Map<String, String> identityMap = new HashMap<>();
        for (SensorsAnalyticsIdentity identity : identities) {
            identityMap.putAll(identity.getIdentityMap());
        }
        if (identityMap.size() < 2) {
            throw new InvalidArgumentException("The identities is invalid，you should have at least two identities.");
        }
        addEventIdentity(identityMap, BIND_ID_ACTION_TYPE, BIND_ID, null);
    }

    @Override
    public void unbind(@NonNull SensorsAnalyticsIdentity analyticsIdentity) throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity.getIdentityMap(), UNBIND_ID_ACTION_TYPE, UNBIND_ID, null);
    }

    @Override
    public void trackById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String eventName,
        Map<String, Object> properties) throws InvalidArgumentException {
        IDMEventRecord eventRecord = IDMEventRecord.starter()
            .identityMap(analyticsIdentity.getIdentityMap())
            .setEventName(eventName)
            .addProperties(properties)
            .build();
        worker.doAddData(new SensorsData(eventRecord));
    }

    @Override
    public void profileSetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        addProfileIdentity(analyticsIdentity, PROFILE_SET_ACTION_TYPE,  properties);
    }

    @Override
    public void profileSetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addProfileIdentity(analyticsIdentity, PROFILE_SET_ACTION_TYPE, properties);
    }

    @Override
    public void profileSetOnceById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        addProfileIdentity(analyticsIdentity, PROFILE_SET_ONCE_ACTION_TYPE, properties);
    }

    @Override
    public void profileSetOnceById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addProfileIdentity(analyticsIdentity, PROFILE_SET_ONCE_ACTION_TYPE,  properties);
    }

    @Override
    public void profileIncrementById(@NonNull SensorsAnalyticsIdentity analyticsIdentity,
        Map<String, Object> properties) throws InvalidArgumentException {
        addProfileIdentity(analyticsIdentity, PROFILE_INCREMENT_ACTION_TYPE, properties);
    }

    @Override
    public void profileIncrementById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, String property, long value)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addProfileIdentity(analyticsIdentity, PROFILE_INCREMENT_ACTION_TYPE,  properties);
    }

    @Override
    public void profileAppendById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        addProfileIdentity(analyticsIdentity, PROFILE_APPEND_ACTION_TYPE, properties);
    }

    @Override
    public void profileAppendById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull String value) throws InvalidArgumentException {
        List<String> values = new ArrayList<>();
        values.add(value);
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, values);
        addProfileIdentity(analyticsIdentity, PROFILE_APPEND_ACTION_TYPE, properties);
    }

    @Override
    public void profileUnsetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            if (!PROJECT_SYSTEM_ATTR.equals(property.getKey())) {
                if (property.getValue() instanceof Boolean) {
                    boolean value = (Boolean) property.getValue();
                    if (value) {
                        continue;
                    }
                }
                throw new InvalidArgumentException("The property value of [" + property.getKey() + "] should be true.");
            }
        }
        addProfileIdentity(analyticsIdentity, PROFILE_UNSET_ACTION_TYPE, properties);
    }

    @Override
    public void profileUnsetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, true);
        addProfileIdentity(analyticsIdentity, PROFILE_UNSET_ACTION_TYPE, properties);
    }

    @Override
    public void profileDeleteById(@NonNull SensorsAnalyticsIdentity analyticsIdentity) throws InvalidArgumentException {
        addProfileIdentity(analyticsIdentity, PROFILE_DELETE_ACTION_TYPE, null);
    }

    @Override
    public void trackById(@NonNull IDMEventRecord idmEventRecord) throws InvalidArgumentException {
        worker.doAddData(new SensorsData(idmEventRecord));
    }

    @Override
    public void profileSetById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(PROFILE_SET_ACTION_TYPE, idmUserRecord.getPropertyMap());
        worker.doAddData(new SensorsData(idmUserRecord, PROFILE_SET_ACTION_TYPE));
    }

    @Override
    public void profileSetOnceById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(PROFILE_SET_ONCE_ACTION_TYPE, idmUserRecord.getPropertyMap());
        worker.doAddData(new SensorsData(idmUserRecord, PROFILE_SET_ONCE_ACTION_TYPE));
    }

    @Override
    public void profileIncrementById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(PROFILE_INCREMENT_ACTION_TYPE, idmUserRecord.getPropertyMap());
        worker.doAddData(new SensorsData(idmUserRecord, PROFILE_INCREMENT_ACTION_TYPE));
    }

    @Override
    public void profileAppendById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(PROFILE_APPEND_ACTION_TYPE, idmUserRecord.getPropertyMap());
        worker.doAddData(new SensorsData(idmUserRecord, PROFILE_APPEND_ACTION_TYPE));
    }

    @Override
    public void profileUnsetById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(PROFILE_UNSET_ACTION_TYPE, idmUserRecord.getPropertyMap());
        if (idmUserRecord.getPropertyMap() == null) {
            return;
        }
        for (Map.Entry<String, Object> property : idmUserRecord.getPropertyMap().entrySet()) {
            if (!PROJECT_SYSTEM_ATTR.equals(property.getKey())) {
                if (property.getValue() instanceof Boolean) {
                    boolean value = (Boolean) property.getValue();
                    if (value) {
                        continue;
                    }
                }
                throw new InvalidArgumentException("The property value of " + property.getKey() + " should be true.");
            }
        }
        worker.doAddData(new SensorsData(idmUserRecord, PROFILE_UNSET_ACTION_TYPE));
    }


    @Override
    public void track(@NonNull UserEventSchema userEventSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(userEventSchema, TRACK_ACTION_TYPE));
    }

    @Override
    public void track(@NonNull ItemEventSchema itemEventSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(itemEventSchema, TRACK_ACTION_TYPE));
    }

    @Override
    public void bind(@NonNull UserEventSchema userEventSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(userEventSchema, BIND_ID_ACTION_TYPE));
    }

    @Override
    public void unbind(@NonNull UserEventSchema userEventSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(userEventSchema, UNBIND_ID_ACTION_TYPE));
    }

    @Override
    public void profileSet(@NonNull UserSchema userSchema) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertSchemaProperties(userSchema.getPropertyMap(), PROFILE_SET_ACTION_TYPE);
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_SET_ACTION_TYPE));
    }

    @Override
    public void profileSetOnce(@NonNull UserSchema userSchema) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertSchemaProperties(userSchema.getPropertyMap(), PROFILE_SET_ONCE_ACTION_TYPE);
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_SET_ONCE_ACTION_TYPE));
    }

    @Override
    public void profileSetIncrement(@NonNull UserSchema userSchema) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertSchemaProperties(userSchema.getPropertyMap(), PROFILE_INCREMENT_ACTION_TYPE);
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_INCREMENT_ACTION_TYPE));
    }

    @Override
    public void profileAppend(@NonNull UserSchema userSchema) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertSchemaProperties(userSchema.getPropertyMap(), PROFILE_APPEND_ACTION_TYPE);
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_APPEND_ACTION_TYPE));
    }

    @Override
    public void profileUnset(@NonNull UserSchema userSchema) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertSchemaProperties(userSchema.getPropertyMap(), PROFILE_UNSET_ACTION_TYPE);
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_UNSET_ACTION_TYPE));
    }

    @Override
    public void profileDelete(@NonNull SensorsAnalyticsIdentity identity) throws InvalidArgumentException {
        UserSchema userSchema = UserSchema.init().identityMap(identity.getIdentityMap()).start();
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_DELETE_ACTION_TYPE));
    }

    @Override
    public void profileDelete(@NonNull Long userId) throws InvalidArgumentException {
        UserSchema userSchema = UserSchema.init().setUserId(userId).start();
        worker.doSchemaData(new SensorsSchemaData(userSchema, PROFILE_DELETE_ACTION_TYPE));
    }

    @Override
    public void itemSet(@NonNull ItemSchema itemSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(itemSchema, ITEM_SET_ACTION_TYPE));
    }

    @Override
    public void itemDelete(@NonNull ItemSchema itemSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(itemSchema, ITEM_DELETE_ACTION_TYPE));
    }

    @Override
    public void itemSet(@NonNull UserItemSchema userItemSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(userItemSchema, ITEM_SET_ACTION_TYPE));
    }

    @Override
    public void itemDelete(@NonNull UserItemSchema userItemSchema) throws InvalidArgumentException {
        worker.doSchemaData(new SensorsSchemaData(userItemSchema, ITEM_DELETE_ACTION_TYPE));
    }

    @Override
    public void flush() {
        worker.flush();
    }

    @Override
    public void shutdown() {
        worker.shutdown();
    }

    private void dealProfile(String distinctId, Boolean isLoginId, Map<String, Object> properties, String actionType)
        throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        UserRecord userRecord = UserRecord.builder()
            .setDistinctId(distinctId)
            .isLoginId(isLoginId)
            .addProperties(properties)
            .build();
        worker.doAddData(new SensorsData(userRecord, actionType));
    }

    private void dealProfile(UserRecord userRecord, String actionType) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(actionType, userRecord.getPropertyMap());
        worker.doAddData(new SensorsData(userRecord, actionType));
    }

    private void addItem(String itemType, String itemId, String actionType, Map<String, Object> properties)
        throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        ItemRecord itemRecord = ItemRecord.builder()
            .setItemId(itemId)
            .setItemType(itemType)
            .addProperties(properties)
            .build();
        worker.doAddData(new SensorsData(itemRecord, actionType));
    }

    private void addEvent(String distinctId, boolean isLoginId, String originDistinctId, String actionType,
        String eventName, Map<String, Object> properties) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        if (actionType.equals(TRACK_SIGN_UP_ACTION_TYPE)) {
            SensorsAnalyticsUtil.assertValue(ORIGINAL_DISTINCT_ID, originDistinctId);
        }
        EventRecord eventRecord = EventRecord.builder()
            .setEventName(eventName)
            .setDistinctId(distinctId)
            .isLoginId(isLoginId)
            .addProperties(properties)
            .build();
        SensorsData sensorsData = new SensorsData(eventRecord, actionType);
        sensorsData.setOriginalId(originDistinctId);
        worker.doAddData(sensorsData);
    }

    private void addProfileIdentity(SensorsAnalyticsIdentity analyticsIdentity, String actionType,
        Map<String, Object> properties) throws InvalidArgumentException {
        if (analyticsIdentity.getIdentityMap().isEmpty()) {
            throw new InvalidArgumentException("The identity is empty.");
        }
        assertIdentityMap(actionType, analyticsIdentity.getIdentityMap());
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        IDMUserRecord idmUserRecord = IDMUserRecord.starter()
            .identityMap(analyticsIdentity.getIdentityMap())
            .addProperties(properties)
            .build();
        worker.doAddData(new SensorsData(idmUserRecord, actionType));
    }

    private void addEventIdentity(Map<String, String> identityMap, String actionType, String eventName,
        Map<String, Object> properties) throws InvalidArgumentException {
        if (identityMap.isEmpty()) {
            throw new InvalidArgumentException("The identity is empty.");
        }
        assertIdentityMap(actionType, identityMap);
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        IDMEventRecord idmEventRecord = IDMEventRecord.starter()
            .setEventName(eventName)
            .identityMap(identityMap)
            .addProperties(properties)
            .build();
        worker.doAddData(new SensorsData(idmEventRecord, actionType));
    }



    private void assertIdentityMap(String actionType, Map<String, String> identityMap) throws InvalidArgumentException {
        for (Map.Entry<String, String> entry : identityMap.entrySet()) {
            SensorsAnalyticsUtil.assertKey(actionType, entry.getKey());
            SensorsAnalyticsUtil.assertValue(actionType, entry.getValue());
        }
    }

}
