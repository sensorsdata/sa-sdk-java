package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.schema.UserItemSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

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
        .setUserId(USER_ID)
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
        .setUserId(USER_ID)
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
          .setUserId(USER_ID)
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

}
