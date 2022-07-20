package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

import java.util.Date;

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


}
