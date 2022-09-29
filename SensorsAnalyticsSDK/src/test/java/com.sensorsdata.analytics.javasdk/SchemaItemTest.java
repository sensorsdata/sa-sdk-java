package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

/**
 * TODO
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/16 15:28
 */
public class SchemaItemTest extends SensorsBaseTest {

  private static final String ITEM_ID = "test11";

  private static final String SCHEMA = "item_schema";

  /**
   * 生成 itemSchema 格式数据
   */
  @Test
  public void checkItemSet() throws InvalidArgumentException {
    ItemSchema itemSchema = ItemSchema.init()
        .setItemId(ITEM_ID)
        .setSchema(SCHEMA)
        .addProperty("key1", "value1")
        .addProperty("key2", 22)
        .start();
    sa.itemSet(itemSchema);
    assertISData(data);
  }
}
