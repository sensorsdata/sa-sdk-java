package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.schema.IdentitySchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Assert;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * bind/unbind 接口支持自定义属性
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2023/10/18 15:02
 */
public class IDMBindTest extends SensorsBaseTest {

  @Test
  public void testIdMappingBind() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("$identity_mobile", "123")
        .addIdentityProperty("$identity_email", "fz@163.com")
        .build();
    Map<String, Object> properties = new HashMap<>();
    properties.put("$project", "abc");
    properties.put("$time_free", true);
    properties.put("$token", "12345");
    sa.bind(properties, identity);
    assertIDM3EventData(data);
    Assert.assertTrue(data.containsKey("project"));
    Assert.assertTrue(data.containsKey("time_free"));
    Assert.assertTrue(data.containsKey("token"));
    Assert.assertEquals("abc", data.get("project"));
    Assert.assertEquals(true, data.get("time_free"));
    Assert.assertEquals("12345", data.get("token"));
  }


  @Test
  public void testBind() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("$identity_mobile", "123")
        .addIdentityProperty("$identity_email", "fz@163.com")
        .build();
    IdentitySchema schema = IdentitySchema.init()
        .identityMap(identity.getIdentityMap())
        .addProperty("$project", "abc")
        .addProperty("$time_free", true)
        .addProperty("$token", "12345")
        .build();
    sa.bind(schema);
    assertUESData(data);
    Assert.assertTrue(data.containsKey("project"));
    Assert.assertTrue(data.containsKey("time_free"));
    Assert.assertTrue(data.containsKey("token"));
    Assert.assertEquals("abc", data.get("project"));
    Assert.assertEquals(true, data.get("time_free"));
    Assert.assertEquals("12345", data.get("token"));
  }

  @Test
  public void testUnbind() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("$project", "abc");
    properties.put("$time_free", true);
    properties.put("$token", "12345");
    sa.unbind("key1", "value1", properties);
    assertIDM3EventData(data);
    Assert.assertTrue(data.containsKey("project"));
    Assert.assertTrue(data.containsKey("time_free"));
    Assert.assertTrue(data.containsKey("token"));
    Assert.assertEquals("abc", data.get("project"));
    Assert.assertEquals(true, data.get("time_free"));
    Assert.assertEquals("12345", data.get("token"));
  }

  @Test
  public void testUnbindSchema() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("$identity_email", "fz@163.com")
        .build();
    IdentitySchema schema = IdentitySchema.init()
        .identityMap(identity.getIdentityMap())
        .addProperty("$project", "abc")
        .addProperty("$time_free", true)
        .addProperty("$token", "12345")
        .build();
    sa.unbind(schema);
    assertUESData(data);
    Assert.assertTrue(data.containsKey("project"));
    Assert.assertTrue(data.containsKey("time_free"));
    Assert.assertTrue(data.containsKey("token"));
    Assert.assertEquals("abc", data.get("project"));
    Assert.assertEquals(true, data.get("time_free"));
    Assert.assertEquals("12345", data.get("token"));
  }
}
