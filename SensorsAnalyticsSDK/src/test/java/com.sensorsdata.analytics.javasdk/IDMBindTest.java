package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.schema.IdentitySchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("$identity_mobile", "123")
                        .addIdentityProperty("$identity_email", "fz@163.com")
                        .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$project", "abc");
        properties.put("$time_free", true);
        properties.put("$token", "12345");
        sa.bind(properties, identity);
        assertIDM3EventData(data);
        Assertions.assertTrue(data.containsKey("project"));
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(data.containsKey("token"));
        Assertions.assertEquals("abc", data.get("project"));
        Assertions.assertEquals(true, data.get("time_free"));
        Assertions.assertEquals("12345", data.get("token"));
    }

    @Test
    public void testBind() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("$identity_mobile", "123")
                        .addIdentityProperty("$identity_email", "fz@163.com")
                        .build();
        IdentitySchema schema =
                IdentitySchema.init()
                        .identityMap(identity.getIdentityMap())
                        .addProperty("$project", "abc")
                        .addProperty("$time_free", true)
                        .addProperty("$token", "12345")
                        .build();
        sa.bind(schema);
        assertUESData(data);
        Assertions.assertTrue(data.containsKey("project"));
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(data.containsKey("token"));
        Assertions.assertEquals("abc", data.get("project"));
        Assertions.assertEquals(true, data.get("time_free"));
        Assertions.assertEquals("12345", data.get("token"));
    }

    @Test
    public void testUnbind() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$project", "abc");
        properties.put("$time_free", true);
        properties.put("$token", "12345");
        sa.unbind("key1", "value1", properties);
        assertIDM3EventData(data);
        Assertions.assertTrue(data.containsKey("project"));
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(data.containsKey("token"));
        Assertions.assertEquals("abc", data.get("project"));
        Assertions.assertEquals(true, data.get("time_free"));
        Assertions.assertEquals("12345", data.get("token"));
    }

    @Test
    public void testUnbindSchema() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("$identity_email", "fz@163.com")
                        .build();
        IdentitySchema schema =
                IdentitySchema.init()
                        .identityMap(identity.getIdentityMap())
                        .addProperty("$project", "abc")
                        .addProperty("$time_free", true)
                        .addProperty("$token", "12345")
                        .build();
        sa.unbind(schema);
        assertUESData(data);
        Assertions.assertTrue(data.containsKey("project"));
        Assertions.assertTrue(data.containsKey("time_free"));
        Assertions.assertTrue(data.containsKey("token"));
        Assertions.assertEquals("abc", data.get("project"));
        Assertions.assertEquals(true, data.get("time_free"));
        Assertions.assertEquals("12345", data.get("token"));
    }
}
