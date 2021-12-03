package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.*;
import com.sensorsdata.analytics.javasdk.consumer.Consumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.sensorsdata.analytics.javasdk.SensorsConst.*;

/**
 * Sensors Analytics SDK
 */
public class SensorsAnalytics implements ISensorsAnalytics {

    private final SensorsAnalyticsWorker worker;

    private static final String DISTINCT_ID = "distinct_id";
    private static final String EVENT_NAME = "event name";
    private static final String ORIGINAL_DISTINCT_ID = "Original Distinct Id";
    private static final String ITEM_TYPE = "Item Type";
    private static final String ITEM_ID = "Item Id";

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
        addEvent(distinctId, isLoginId, null, PROFILE_SET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSet(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addEvent(distinctId, isLoginId, null, PROFILE_SET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetOnce(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, PROFILE_SET_ONCE_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetOnce(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addEvent(distinctId, isLoginId, null, PROFILE_SET_ONCE_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileIncrement(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, PROFILE_INCREMENT_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileIncrement(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull long value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addEvent(distinctId, isLoginId, null, PROFILE_INCREMENT_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileAppend(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, PROFILE_APPEND_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileAppend(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull String value) throws InvalidArgumentException {
        List<String> values = new ArrayList<>();
        values.add(value);
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, values);
        addEvent(distinctId, isLoginId, null, PROFILE_APPEND_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileUnset(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, true);
        addEvent(distinctId, isLoginId, null, PROFILE_UNSET_ACTION_TYPE, null, properties);
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
        addEvent(distinctId, isLoginId, null, PROFILE_UNSET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileDelete(@NonNull String distinctId, @NonNull boolean isLoginId) throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, PROFILE_DELETE_ACTION_TYPE, null, null);
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
        assertIdentityMap(BIND_ID_ACTION_TYPE, identityMap);
        worker.doAddEventIdentity(identityMap, BIND_ID_ACTION_TYPE, BIND_ID, null);
    }

    @Override
    public void unbind(@NonNull SensorsAnalyticsIdentity analyticsIdentity) throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, UNBIND_ID_ACTION_TYPE, UNBIND_ID, null);
    }

    @Override
    public void trackById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String eventName,
        Map<String, Object> properties) throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, TRACK_ACTION_TYPE, eventName, properties);
    }

    @Override
    public void profileSetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, PROFILE_SET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addEventIdentity(analyticsIdentity, PROFILE_SET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetOnceById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, PROFILE_SET_ONCE_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetOnceById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addEventIdentity(analyticsIdentity, PROFILE_SET_ONCE_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileIncrementById(@NonNull SensorsAnalyticsIdentity analyticsIdentity,
        Map<String, Object> properties) throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, PROFILE_INCREMENT_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileIncrementById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, String property, long value)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, value);
        addEventIdentity(analyticsIdentity, PROFILE_INCREMENT_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileAppendById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, PROFILE_APPEND_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileAppendById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull String value) throws InvalidArgumentException {
        List<String> values = new ArrayList<String>();
        values.add(value);
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, values);
        addEventIdentity(analyticsIdentity, PROFILE_APPEND_ACTION_TYPE, null, properties);
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
        addEventIdentity(analyticsIdentity, PROFILE_UNSET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileUnsetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put(property, true);
        addEventIdentity(analyticsIdentity, PROFILE_UNSET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileDeleteById(@NonNull SensorsAnalyticsIdentity analyticsIdentity) throws InvalidArgumentException {
        addEventIdentity(analyticsIdentity, PROFILE_DELETE_ACTION_TYPE, null, null);
    }

    @Override
    public void flush() {
        worker.flush();
    }

    @Override
    public void shutdown() {
        worker.shutdown();
    }

    private void dealProfile(UserRecord userRecord, String actionType) throws InvalidArgumentException {
        addEvent(userRecord.getDistinctId(), userRecord.getIsLoginId(), null, actionType, null,
            userRecord.getPropertyMap());
    }

    private void addItem(String itemType, String itemId, String actionType, Map<String, Object> properties)
        throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertKey(ITEM_TYPE, itemType);
        SensorsAnalyticsUtil.assertValue(ITEM_ID, itemId);
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        worker.doAddItem(itemType, itemId, actionType, properties);
    }

    private void addEvent(String distinctId, boolean isLoginId, String originDistinctId, String actionType,
        String eventName, Map<String, Object> properties) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertValue(DISTINCT_ID, distinctId);
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        if (actionType.equals(TRACK_ACTION_TYPE)) {
            SensorsAnalyticsUtil.assertKey(EVENT_NAME, eventName);
        } else if (actionType.equals(TRACK_SIGN_UP_ACTION_TYPE)) {
            SensorsAnalyticsUtil.assertValue(ORIGINAL_DISTINCT_ID, originDistinctId);
        }
        worker.doAddEvent(distinctId, isLoginId, originDistinctId, actionType, eventName, properties);
    }

    private void addEventIdentity(SensorsAnalyticsIdentity analyticsIdentity, String actionType, String eventName,
        Map<String, Object> properties) throws InvalidArgumentException {
        Map<String, String> identityMap = analyticsIdentity.getIdentityMap();
        if (identityMap.isEmpty()) {
            throw new InvalidArgumentException("The identity is empty.");
        }
        assertIdentityMap(actionType, identityMap);
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        if (actionType.equals(TRACK_ACTION_TYPE)) {
            SensorsAnalyticsUtil.assertKey(EVENT_NAME, eventName);
        }
        worker.doAddEventIdentity(identityMap, actionType, eventName, properties);
    }

    private void assertIdentityMap(String actionType, Map<String, String> identityMap) throws InvalidArgumentException {
        for (Map.Entry<String, String> entry : identityMap.entrySet()) {
            SensorsAnalyticsUtil.assertKey(actionType, entry.getKey());
            SensorsAnalyticsUtil.assertValue(actionType, entry.getValue());
        }
    }

}
