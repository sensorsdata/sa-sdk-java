package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.schema.IdentitySchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Date;
import java.util.Map;

/**
 * userEventSchema 单元测试
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/15 10:23
 */
public class SchemaUserEventTest extends SensorsBaseTest {

  private static final Long USER_ID = 12345L;

  private static final String EVENT_NAME = "testEvent";

  private static final String DISTINCT_ID = "fz123";

  /**
   * 使用 userEventSchema 生成带 userId 数据
   * 期望：生成数据格式中用户信息节点为 userId
   */
  @Test
  public void checkUserEventSchemaWithUserId() throws InvalidArgumentException {
    UserEventSchema userEventSchema = UserEventSchema.init()
        .setUserId(USER_ID)
        .setEventName(EVENT_NAME)
        .addProperty("key1", "value1")
        .addProperty("key2", 22)
        .addProperty("$time", new Date())
        .start();
    sa.track(userEventSchema);
    assertUESData(data);
  }

  /**
   * 构建携带 identities 的用户信息，distinct_id 值与 IDM3.0 生成逻辑保持一致
   * 期望：生成数据中 properties 内有 identities + distinct_id 节点；并且 distinct_id 的值为 $identity_login_id 的值
   */
  @Test
  public void checkUserEventSchemaWithIdentities() throws InvalidArgumentException {
    UserEventSchema userEventSchema = UserEventSchema.init()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, DISTINCT_ID)
        .setEventName(EVENT_NAME)
        .start();
    sa.track(userEventSchema);
    assertUESData(data);
    assertEventIdentitiesInfo(data, DISTINCT_ID);
  }

  /**
   * 校验用户绑定事件
   */
  @Test
  public void checkSchemaBind() throws InvalidArgumentException {
    IdentitySchema identitySchema = IdentitySchema.init().build();
    try {
      sa.bind(identitySchema);
      fail("生成异常数据");
    } catch (InvalidArgumentException e) {
      assertTrue(e.getMessage().contains("The identities is invalid，you should have at least two identities."));
    }

    IdentitySchema oneIdentity = IdentitySchema.init()
        .addIdentityProperty("key1", "value1")
        .build();
    try {
      sa.bind(oneIdentity);
      fail("生成异常数据");
    } catch (InvalidArgumentException e) {
      assertTrue(e.getMessage().contains("The identities is invalid，you should have at least two identities."));
    }

    IdentitySchema moreIdentity = IdentitySchema.init()
        .addIdentityProperty("key1", "value1")
        .addIdentityProperty("key2", "value2")
        .build();
    sa.bind(moreIdentity);
    assertUESData(data);
  }

  @Test
  public void checkSchemaUnbind() throws InvalidArgumentException {
    IdentitySchema identitySchema = IdentitySchema.init().build();
    try {
      sa.unbind(identitySchema);
      fail("生成异常数据");
    } catch (InvalidArgumentException e) {
      assertTrue(e.getMessage().contains("unbind user operation cannot input multiple or none identifiers"));
    }

    IdentitySchema moreIdentity = IdentitySchema.init()
        .addIdentityProperty("key1", "value1")
        .addIdentityProperty("key2", "value2")
        .build();

    try {
      sa.unbind(moreIdentity);
      fail("生成异常数据");
    } catch (InvalidArgumentException e) {
      assertTrue(e.getMessage().contains("unbind user operation cannot input multiple or none identifiers"));
    }


    IdentitySchema oneIdentity = IdentitySchema.init()
        .addIdentityProperty("key1", "value1")
        .build();
    sa.unbind(oneIdentity);
    assertUESData(data);


  }


  //----------------------------------v3.5.1--------------------------------------

  /**
   * 支持 user 数据传入 userId 作为用户标识
   * <p>期望：用户数据传入 userId,最终节点中包含 userId 和 distinctId 信息 </p>
   */
  @Test
  public void checkUserId() throws InvalidArgumentException {
    UserEventSchema userEventSchema = UserEventSchema.init()
        .setUserId(123L)
        .setEventName(EVENT_NAME)
        .start();
    sa.track(userEventSchema);
    assertUESData(data);
  }

  /**
   * 同时传入 userID 和 identities 节点
   * <p>期望：userId 优先级最高，两者同时传入，最终数据中存在 userId </p>
   */
  @Test
  public void checkUserIdAndIdentities() throws InvalidArgumentException {
    UserEventSchema userEventSchema = UserEventSchema.init()
        .setUserId(123L)
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "eee")
        .setEventName(EVENT_NAME)
        .start();
    sa.track(userEventSchema);
    assertUESData(data);
    Map<String, Object> properties = (Map<String, Object>) data.get("properties");
    assertTrue(properties.containsKey("user_id"));
    assertEquals(123L, properties.get("user_id"));
  }

  /**
   * 同时传入 userID 和 distinctId 节点
   * <p>期望：最终数据节点中，以传入的 distinctId 为主 </p>
   */
  @Test
  public void checkUserIdAndDistinctId() throws InvalidArgumentException {
    UserEventSchema userEventSchema = UserEventSchema.init()
        .setUserId(123L)
        .setDistinctId("test")
        .setEventName(EVENT_NAME)
        .start();
    sa.track(userEventSchema);
    assertUESData(data);
    Map<String, Object> properties = (Map<String, Object>) data.get("properties");
    Assert.assertTrue(properties.containsKey("distinct_id"));
    Assert.assertEquals("test", properties.get("distinct_id"));
  }

}
