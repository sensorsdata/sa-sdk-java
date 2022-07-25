package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

/**
 * TODO
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/16 17:44
 */
public class SchemaItemEventTest extends SensorsBaseTest {

  private static final String SCHEMA = "product_event";

  private static final String EVENT_NAME = "cart";

  @Test
  public void checkItemEvent() throws InvalidArgumentException {
    ItemEventSchema itemEventSchema = ItemEventSchema.init()
        .setSchema(SCHEMA)
        .setEventName(EVENT_NAME)
        .addProperty("key1", "value1")
        .start();
    sa.track(itemEventSchema);
    assertIESData(data);
  }

  @Test
  public void checkInvalidEventItemKey() {
    try {
      ItemEventSchema itemEventSchema = ItemEventSchema.init()
          .setEventName("aa")
          .setSchema("sss")
          .addProperty("11age", "value")
          .start();
      sa.track(itemEventSchema);
      fail("生成异常数据");
    } catch (InvalidArgumentException e) {
      assertTrue(e.getMessage().contains("is invalid."));
    }
  }
}
