package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

/**
 * Id-Mapping 模式
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/18 23:33
 */
public class IdMappingModelTest extends SensorsBaseTest {

  /**
   * 校验 id-mapping 模式生成事件节点属性是否完整
   */
  @Test
  public void checkIdMappingEvent() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("login_id", "123")
        .build();
    Map<String, Object> properties = new HashMap<>();
    properties.put("test", "test");
    properties.put("$project", "abc");
    properties.put("$token", "123");
    sa.trackById(identity, "test", properties);
    assertEquals(1, res.size());
    assertNotNull(res.get(0).get("identities"));
    assertNotNull(res.get(0).get("time"));
    assertNotNull(res.get(0).get("_track_id"));
    assertEquals("test", res.get(0).get("event"));
    assertEquals("123", res.get(0).get("distinct_id"));
    assertNotNull(res.get(0).get("properties"));
    assertNotNull(res.get(0).get("project"));
    assertNotNull(res.get(0).get("token"));
  }

  /**
   * 校验 ID-Mapping 上报多维度用户 ID
   */
  @Test
  public void checkIdMappingTrackMoreId() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("$identity_mobile", "123")
        .addIdentityProperty("$identity_email", "fz@163.com")
        .build();
    sa.trackById(identity, "view", null);
    assertNotNull(res.get(0).get("identities"));
    assertTrue(res.get(0).get("identities") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("identities");
    assertEquals(2, result.size());
    assertEquals("123", result.get("$identity_mobile"));
    assertEquals("fz@163.com", result.get("$identity_email"));
  }

  /**
   * 校验 ID-Mapping bind 接口
   */
  @Test
  public void checkIdMappingBind() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("$identity_mobile", "123")
        .addIdentityProperty("$identity_email", "fz@163.com")
        .build();
    sa.bind(identity);
    assertNotNull(res.get(0).get("identities"));
    assertTrue(res.get(0).get("identities") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("identities");
    assertEquals(2, result.size());
    assertEquals("123", result.get("$identity_mobile"));
    assertEquals("fz@163.com", result.get("$identity_email"));
    assertEquals("$BindID", res.get(0).get("event"));
    assertEquals("track_id_bind", res.get(0).get("type"));
  }

  /**
   * 校验 ID—Mapping bind 接口单用户属性
   */
  @Test
  public void checkIdMappingBindOneId() {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("$identity_mobile", "123")
        .build();
    try {
      sa.bind(identity);
      fail();
    } catch (InvalidArgumentException e) {
      assertTrue(true);
    }
  }

  /**
   * 校验 ID-Mapping unbind 接口用户格式
   */
  @Test
  public void checkUnbindUserId() throws InvalidArgumentException {
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("id_test1", "id_value1")
        .build();
    sa.unbind(identity);
    assertNotNull(res.get(0).get("identities"));
    assertTrue(res.get(0).get("identities") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("identities");
    assertEquals("id_value1", result.get("id_test1"));
    assertEquals("track_id_unbind", res.get(0).get("type"));
  }

  /**
   * 校验 ID-Mapping 公共属性
   */
  @Test
  public void checkTrackByIdSuperProperties() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("asd", "123");
    sa.registerSuperProperties(properties);
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("id_test1", "id_value1")
        .build();
    sa.trackById(identity, "eee", null);
    assertTrue(res.get(0).get("properties") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
    assertEquals("123", result.get("asd"));
  }

  /**
   * 校验 ID-Mapping bind 接口公共属性
   */
  @Test
  public void checkBindSuperProperties() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("asd", "123");
    sa.registerSuperProperties(properties);
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("id_test1", "id_value1")
        .addIdentityProperty("eee", "123")
        .build();
    sa.bind(identity);
    assertTrue(res.get(0).get("properties") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
    assertEquals("123", result.get("asd"));
  }

  /**
   * 校验 ID_Mapping unbind 接口公共属性
   */
  @Test
  public void checkUnbindSuperProperties() throws InvalidArgumentException {
    Map<String, Object> properties = new HashMap<>();
    properties.put("asd", "123");
    sa.registerSuperProperties(properties);
    SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty("id_test1", "id_value1")
        .build();
    sa.unbind(identity);
    assertTrue(res.get(0).get("properties") instanceof Map);
    Map<?, ?> result = (Map<?, ?>) res.get(0).get("properties");
    assertEquals("123", result.get("asd"));
  }
}
