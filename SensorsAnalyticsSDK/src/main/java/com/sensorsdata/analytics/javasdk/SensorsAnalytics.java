package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SuperPropertiesRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.consumer.Consumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Sensors Analytics SDK
 */
public class SensorsAnalytics implements ISensorsAnalytics {

    private final Consumer consumer;
    private final Map<String, Object> superProperties;
    private boolean enableTimeFree = false;

    public SensorsAnalytics(final Consumer consumer) {
        this.consumer = consumer;
        this.superProperties = new ConcurrentHashMap<String, Object>();
        clearSuperProperties();

        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
            @Override
            public void run() {
                flush();
            }
        }));
    }

    public boolean isEnableTimeFree() {
        return enableTimeFree;
    }

    @Override
    public void setEnableTimeFree(boolean enableTimeFree) {
        this.enableTimeFree = enableTimeFree;
    }

    /**
     * 设置公共属性
     *
     * @param propertiesRecord 公共属性实体
     */
    @Override
    public void registerSuperProperties(SuperPropertiesRecord propertiesRecord) {
        for (String key : propertiesRecord.getPropertyMap().keySet()) {
            this.superProperties.put(key, propertiesRecord.getPropertyMap().get(key));
        }
    }

    @Override
    public void registerSuperProperties(Map<String, Object> superPropertiesMap) {
        for (Map.Entry<String, Object> item : superPropertiesMap.entrySet()) {
            this.superProperties.put(item.getKey(), item.getValue());
        }
    }

    @Override
    public void clearSuperProperties() {
        this.superProperties.clear();
        this.superProperties.put(SensorsConst.LIB_SYSTEM_ATTR, SensorsConst.LIB);
        this.superProperties.put(SensorsConst.LIB_VERSION_SYSTEM_ATTR, SensorsConst.SDK_VERSION);
    }

    @Override
    public void track(EventRecord eventRecord) throws InvalidArgumentException {
        addEvent(eventRecord.getDistinctId(), eventRecord.getIsLoginId(), null, SensorsConst.TRACK_ACTION_TYPE,
            eventRecord.getEventName(),
            eventRecord.getPropertyMap());
    }

    @Override
    public void profileSet(UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, SensorsConst.PROFILE_SET_ACTION_TYPE);
    }

    @Override
    public void profileSetOnce(UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, SensorsConst.PROFILE_SET_ONCE_ACTION_TYPE);
    }

    @Override
    public void profileAppend(UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, SensorsConst.PROFILE_APPEND_ACTION_TYPE);
    }

    @Override
    public void profileIncrement(UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, SensorsConst.PROFILE_INCREMENT_ACTION_TYPE);
    }

    @Override
    public void profileUnset(UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, SensorsConst.PROFILE_UNSET_ACTION_TYPE);
    }

    @Override
    public void profileDelete(UserRecord userRecord) throws InvalidArgumentException {
        dealProfile(userRecord, SensorsConst.PROFILE_DELETE_ACTION_TYPE);
    }

    @Override
    public void itemSet(ItemRecord itemRecord) throws InvalidArgumentException {
        addItem(itemRecord.getItemType(), itemRecord.getItemId(), SensorsConst.ITEM_SET_ACTION_TYPE,
            itemRecord.getPropertyMap());
    }

    @Override
    public void itemDelete(ItemRecord itemRecord) throws InvalidArgumentException {
        addItem(itemRecord.getItemType(), itemRecord.getItemId(), SensorsConst.ITEM_DELETE_ACTION_TYPE,
            itemRecord.getPropertyMap());
    }

    private void dealProfile(UserRecord userRecord, String actionType) throws InvalidArgumentException {
        addEvent(userRecord.getDistinctId(), userRecord.getIsLoginId(), null, actionType, null,
            userRecord.getPropertyMap());
    }

    @Override
    public void track(String distinctId, boolean isLoginId, String eventName) throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.TRACK_ACTION_TYPE, eventName, null);
    }

    @Override
    public void track(String distinctId, boolean isLoginId, String eventName, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.TRACK_ACTION_TYPE, eventName, properties);
    }

    @Override
    public void trackSignUp(String loginId, String anonymousId) throws InvalidArgumentException {
        addEvent(loginId, false, anonymousId, SensorsConst.TRACK_SIGN_UP_ACTION_TYPE,
            SensorsConst.SIGN_UP_SYSTEM_ATTR, null);
    }

    @Override
    public void trackSignUp(String loginId, String anonymousId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(loginId, false, anonymousId, SensorsConst.TRACK_SIGN_UP_ACTION_TYPE,
            SensorsConst.SIGN_UP_SYSTEM_ATTR, properties);
    }

    @Override
    public void profileSet(String distinctId, boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_SET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSet(String distinctId, boolean isLoginId, String property, Object value)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(property, value);
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_SET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetOnce(String distinctId, boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_SET_ONCE_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileSetOnce(String distinctId, boolean isLoginId, String property, Object value)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(property, value);
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_SET_ONCE_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileIncrement(String distinctId, boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_INCREMENT_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileIncrement(String distinctId, boolean isLoginId, String property, long value)
        throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(property, value);
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_INCREMENT_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileAppend(String distinctId, boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_APPEND_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileAppend(String distinctId, boolean isLoginId, String property, String value)
        throws InvalidArgumentException {
        List<String> values = new ArrayList<String>();
        values.add(value);
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(property, values);
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_APPEND_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileUnset(String distinctId, boolean isLoginId, String property) throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put(property, true);
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_UNSET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileUnset(String distinctId, boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException {
        if (properties == null) {
            return;
        }
        for (Map.Entry<String, Object> property : properties.entrySet()) {
            if (!SensorsConst.PROJECT_SYSTEM_ATTR.equals(property.getKey())) {
                if (property.getValue() instanceof Boolean) {
                    boolean value = (Boolean) property.getValue();
                    if (value) {
                        continue;
                    }
                }
                throw new InvalidArgumentException("The property value of " + property.getKey() + " should be true.");
            }
        }
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_UNSET_ACTION_TYPE, null, properties);
    }

    @Override
    public void profileDelete(String distinctId, boolean isLoginId) throws InvalidArgumentException {
        addEvent(distinctId, isLoginId, null, SensorsConst.PROFILE_DELETE_ACTION_TYPE, null,
            new HashMap<String, Object>());
    }

    @Override
    public void itemSet(String itemType, String itemId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addItem(itemType, itemId, SensorsConst.ITEM_SET_ACTION_TYPE, properties);
    }

    @Override
    public void itemDelete(String itemType, String itemId, Map<String, Object> properties)
        throws InvalidArgumentException {
        addItem(itemType, itemId, SensorsConst.ITEM_DELETE_ACTION_TYPE, properties);
    }

    @Override
    public void flush() {
        this.consumer.flush();
    }

    @Override
    public void shutdown() {
        this.consumer.close();
    }

    private void addEvent(String distinctId, boolean isLoginId, String originDistinctId,
        String actionType, String eventName, Map<String, Object> properties)
        throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertValue("Distinct Id", distinctId);
        SensorsAnalyticsUtil.assertProperties(actionType, properties);
        if (actionType.equals(SensorsConst.TRACK_ACTION_TYPE)) {
            SensorsAnalyticsUtil.assertKey("Event Name", eventName);
        } else if (actionType.equals(SensorsConst.TRACK_SIGN_UP_ACTION_TYPE)) {
            SensorsAnalyticsUtil.assertValue("Original Distinct Id", originDistinctId);
        }

        // Event time
        long time = System.currentTimeMillis();
        Map<String, Object> newProperties = null;
        if (properties != null) {
            newProperties = new HashMap<String, Object>(properties);
        }
        if (newProperties != null && newProperties.containsKey(SensorsConst.TINE_SYSTEM_ATTR)) {
            Date eventTime = (Date) newProperties.get(SensorsConst.TINE_SYSTEM_ATTR);
            newProperties.remove(SensorsConst.TINE_SYSTEM_ATTR);
            time = eventTime.getTime();
        }

        String eventProject = null;
        String eventToken = null;
        if (newProperties != null) {
            if (newProperties.containsKey(SensorsConst.PROJECT_SYSTEM_ATTR)) {
                eventProject = (String) newProperties.get(SensorsConst.PROJECT_SYSTEM_ATTR);
                newProperties.remove(SensorsConst.PROJECT_SYSTEM_ATTR);
            }
            if (newProperties.containsKey(SensorsConst.TOKEN_SYSTEM_ATTR)) {
                eventToken = (String) newProperties.get(SensorsConst.TOKEN_SYSTEM_ATTR);
                newProperties.remove(SensorsConst.TOKEN_SYSTEM_ATTR);
            }
        }

        Map<String, Object> eventProperties = new HashMap<String, Object>();
        if (actionType.equals(SensorsConst.TRACK_ACTION_TYPE) ||
                actionType.equals(SensorsConst.TRACK_SIGN_UP_ACTION_TYPE)) {
            eventProperties.putAll(superProperties);
        }
        if (newProperties != null) {
            eventProperties.putAll(newProperties);
        }

        if (isLoginId) {
            eventProperties.put(SensorsConst.LOGIN_SYSTEM_ATTR, true);
        }

        Map<String, String> libProperties = getLibProperties();

        Map<String, Object> event = new HashMap<String, Object>();

        event.put("type", actionType);
        event.put("time", time);
        event.put("distinct_id", distinctId);
        event.put("properties", eventProperties);
        event.put("lib", libProperties);
        event.put("_track_id", new Random().nextInt());

        if (eventProject != null) {
            event.put("project", eventProject);
        }

        if (eventToken != null) {
            event.put("token", eventToken);
        }

        if (enableTimeFree) {
            event.put("time_free", true);
        }

        if (actionType.equals(SensorsConst.TRACK_ACTION_TYPE)) {
            event.put("event", eventName);
        } else if (actionType.equals(SensorsConst.TRACK_SIGN_UP_ACTION_TYPE)) {
            event.put("event", eventName);
            event.put("original_id", originDistinctId);
        }

        this.consumer.send(event);
    }

    private void addItem(String itemType, String itemId, String actionType,
        Map<String, Object> properties) throws InvalidArgumentException {
        SensorsAnalyticsUtil.assertKey("Item Type", itemType);
        SensorsAnalyticsUtil.assertValue("Item Id", itemId);
        SensorsAnalyticsUtil.assertProperties(actionType, properties);

        Map<String, Object> newProperties = null;
        if (properties != null) {
            newProperties = new HashMap<String, Object>(properties);
        }

        String eventProject = null;
        String eventToken = null;
        if (newProperties != null) {
            if (newProperties.containsKey(SensorsConst.PROJECT_SYSTEM_ATTR)) {
                eventProject = (String) newProperties.get(SensorsConst.PROJECT_SYSTEM_ATTR);
                newProperties.remove(SensorsConst.PROJECT_SYSTEM_ATTR);
            }
            if (newProperties.containsKey(SensorsConst.TOKEN_SYSTEM_ATTR)) {
                eventToken = (String) newProperties.get(SensorsConst.TOKEN_SYSTEM_ATTR);
                newProperties.remove(SensorsConst.TOKEN_SYSTEM_ATTR);
            }
        }

        Map<String, Object> eventProperties = new HashMap<String, Object>();
        if (newProperties != null) {
            eventProperties.putAll(newProperties);
        }

        Map<String, String> libProperties = getLibProperties();

        Map<String, Object> record = new HashMap<String, Object>();
        record.put("type", actionType);
        record.put("time", System.currentTimeMillis());
        record.put("properties", eventProperties);
        record.put("lib", libProperties);

        if (eventProject != null) {
            record.put("project", eventProject);
        }

        if (eventToken != null) {
            record.put("token", eventToken);
        }

        record.put("item_type", itemType);
        record.put("item_id", itemId);
        this.consumer.send(record);
    }


    private Map<String, String> getLibProperties() {
        Map<String, String> libProperties = new HashMap<String, String>();
        libProperties.put(SensorsConst.LIB_SYSTEM_ATTR, SensorsConst.LIB);
        libProperties.put(SensorsConst.LIB_VERSION_SYSTEM_ATTR, SensorsConst.SDK_VERSION);
        libProperties.put(SensorsConst.LIB_METHOD_SYSTEM_ATTR, "code");

        if (this.superProperties.containsKey(SensorsConst.APP_VERSION_SYSTEM_ATTR)) {
            libProperties.put(SensorsConst.APP_VERSION_SYSTEM_ATTR,
                (String) this.superProperties.get(SensorsConst.APP_VERSION_SYSTEM_ATTR));
        }

        StackTraceElement[] trace = (new Exception()).getStackTrace();

        if (trace.length > 3) {
            StackTraceElement traceElement = trace[3];
            libProperties.put(SensorsConst.LIB_DETAIL_SYSTEM_ATTR,
                String.format("%s##%s##%s##%s", traceElement.getClassName(),
                    traceElement.getMethodName(), traceElement.getFileName(), traceElement.getLineNumber()));
        }

        return libProperties;
    }
}
