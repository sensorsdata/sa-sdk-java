package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.schema.UserItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * TODO
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/17 14:59
 */
public class SchemaUserItemTest extends SensorsBaseTest {

  private static final String SCHEMA = "cart";
  private static final String ITEM_ID = "test11";
  private static final Long USER_ID = 12345L;

  @Test
  public void checkUserItem() throws InvalidArgumentException {
    UserItemSchema userItemSchema = UserItemSchema.init()
        .setSchema(SCHEMA)
        .setItemId(ITEM_ID)
        .addIdentityProperty("user1", "value1")
        .addProperty("key1", "value1")
        .start();
    sa.itemSet(userItemSchema);
    assertUISData(data);
  }

  @Test
  public void checkSelfTrackId() throws InvalidArgumentException {
    UserItemSchema userItemSchema = UserItemSchema.init()
        .setSchema(SCHEMA)
        .setItemId(ITEM_ID)
        .addIdentityProperty("user1", "value1")
        .addProperty("key1", "value1")
        .addProperty(SensorsConst.TRACK_ID, 12)
        .start();
    sa.itemSet(userItemSchema);
    assertUISData(data);
  }

  @Test
  public void checkSelfProperties() {
    try {
      UserItemSchema userItemSchema = UserItemSchema.init()
          .setSchema(SCHEMA)
          .setItemId(ITEM_ID)
          .addIdentityProperty("user1", "value1")
          .addProperty("11age", "22")
          .start();
      sa.itemSet(userItemSchema);
      fail("生成异常数据");
    } catch (InvalidArgumentException e) {
      assertTrue(e.getMessage().contains("is invalid."));
    }
  }

  @Test
  public void checkIdentities() throws InvalidArgumentException {
    UserItemSchema userItemSchema = UserItemSchema.init()
        .setItemId("eee")
        .setSchema("www")
        .addIdentityProperty("key1", "value1")
        .addIdentityProperty("key2", "value2")
        .start();
    sa.itemSet(userItemSchema);
    assertUISData(data);
  }


  //----------------------------------v3.5.1--------------------------------------

  /**
   * 支持 user 数据传入 userId 作为用户标识
   * <p>期望：用户数据传入 userId,最终节点中包含 userId 和 distinctId 信息 </p>
   */
  @Test
  public void checkUserId() throws InvalidArgumentException {
    UserItemSchema userItemSchema = UserItemSchema.init()
        .setItemId("eee")
        .setSchema("www")
        .setUserId(123L)
        .start();
    sa.itemSet(userItemSchema);
    assertUISData(data);
  }

  /**
   * 同时传入 userID 和 identities 节点
   * <p>期望：userId 优先级最高，两者同时传入，最终数据中存在 userId </p>
   */
  @Test
  public void checkUserIdAndIdentities() throws InvalidArgumentException {
    UserItemSchema userItemSchema = UserItemSchema.init()
        .setItemId("eee")
        .setSchema("www")
        .setUserId(123L)
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "login_id123")
        .start();
    sa.itemSet(userItemSchema);
    assertUISData(data);
    Map<String, Object> properties = (Map<String, Object>) data.get("properties");
    assertTrue(properties.containsKey("user_id"));
  }

  /**
   * 同时传入 userID 和 distinctId 节点
   * <p>期望：最终数据节点中，以传入的 distinctId 为主 </p>
   */
  @Test
  public void checkUserIdAndDistinctId() throws InvalidArgumentException {
    UserSchema userSchema = UserSchema.init()
        .setUserId(123L)
        .setDistinctId("test")
        .addIdentityProperty("key1", "value1")
        .addProperty("$project", "abc")
        .start();
    sa.profileSet(userSchema);
    assertUSData(data);
    Assert.assertTrue(data.containsKey("distinct_id"));
    Assert.assertEquals("test", data.get("distinct_id"));
  }
}
