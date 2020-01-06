package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import java.io.Serializable;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EventMessage implements Serializable {
  private static final long serialVersionUID = -2327319579147636283L;
  private Map<String, Object> propertyMap;
  private String eventName;
  private String loginId;
  private String anonymousId;

  private EventMessage(String eventName, String loginId, String anonymousId, Map<String, Object> propertyMap) {
    this.eventName = eventName;
    this.loginId = loginId;
    this.anonymousId = anonymousId;
    this.propertyMap = propertyMap;
  }

  @Override
  public String toString() {
    return "EventMessage{" +
        "propertyMap=" + propertyMap +
        ", eventName='" + eventName + '\'' +
        ", loginId='" + loginId + '\'' +
        ", anonymousId='" + anonymousId + '\'' +
        '}';
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public Map<String, Object> getPropertyMap() {
    return propertyMap;
  }

  public String getEventName() {
    return eventName;
  }

  public String getLoginId() {
    return loginId;
  }

  public String getAnonymousId() {
    return anonymousId;
  }

  public static class Builder {
    private Map<String, Object> propertyMap = new HashMap();
    private String eventName;
    private String loginId;
    private String anonymousId;

    private Builder() {
    }

    public EventMessage build() throws InvalidArgumentException {
      if (eventName == null) {
        throw new InvalidArgumentException("The eventName is empty.");
      }
      if (loginId == null && anonymousId == null) {
        throw new InvalidArgumentException("The loginId and anonymousId are both empty.");
      }
      return new EventMessage(eventName, loginId, anonymousId, propertyMap);
    }

    public EventMessage.Builder eventName(String eventName) {
      this.eventName  = eventName;
      return this;
    }

    public EventMessage.Builder loginId(String loginId) {
      this.loginId = loginId;
      return this;
    }

    public EventMessage.Builder anonymousId(String anonymousId) {
      this.anonymousId = anonymousId;
      return this;
    }

    public EventMessage.Builder addProperties(Map<String, Object> properties) {
      if (properties != null) {
        propertyMap.putAll(properties);
      }
      return this;
    }

    public EventMessage.Builder addProperty(String key, String property) {
      addPropertyObject(key, property);
      return this;
    }

    public EventMessage.Builder addProperty(String key, boolean property) {
      addPropertyObject(key, property);
      return this;
    }

    public EventMessage.Builder addProperty(String key, Number property) {
      addPropertyObject(key, property);
      return this;
    }

    public EventMessage.Builder addProperty(String key, Date property) {
      addPropertyObject(key, property);
      return this;
    }

    public EventMessage.Builder addProperty(String key, List property) {
      addPropertyObject(key, property);
      return this;
    }

    private EventMessage.Builder addPropertyObject(String key, Object property) {
      if (key != null) {
        propertyMap.put(key, property);
      }
      return this;
    }
  }
}
