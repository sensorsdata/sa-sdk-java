package com.sensorsdata.analytics.javasdk.bean.schema;

import lombok.Getter;

import java.util.HashMap;
import java.util.Map;

/**
 * identitySchema
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/07/25 17:30
 */
@Getter
public class IdentitySchema {

  private Map<String, String> idMap = new HashMap<>();

  protected IdentitySchema(Map<String, String> idMap) {
    this.idMap = idMap;
  }

  public static IdentitySchema.Builder init() {
    return new IdentitySchema.Builder();
  }


  public static class Builder {

    private Map<String, String> idMap = new HashMap<>();

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

    public IdentitySchema build() {
      return new IdentitySchema(idMap);
    }
  }
}
