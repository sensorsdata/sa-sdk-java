package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.schema.DetailSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.Date;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * detail 明细数据单元测试
 *
 * <p>v3.6.3+ 版本开始支持
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2023/03/21 15:42
 */
public class SchemaDetailTest extends SensorsBaseTest {

    /** 测试 detail schema 数据生成逻辑是否正确 */
    @Test
    public void checkDetailSet() throws InvalidArgumentException {
        DetailSchema detailSchema =
                DetailSchema.init()
                        .setSchema("test")
                        .setDetailId("ee")
                        .addProperty("key1", "value1")
                        .start();
        sa.detailSet(detailSchema);
        assertSchemaDataNode(data);
    }

    /** 测试用户实体明细数据生成逻辑是否正常 */
    @Test
    public void checkUserDetailSet() throws InvalidArgumentException {
        SensorsAnalyticsIdentity sensorsIdentities =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("user_key1", "user_value1")
                        .addIdentityProperty("user_key2", "user_value2")
                        .build();
        DetailSchema detailSchema =
                DetailSchema.init()
                        .setSchema("test")
                        .setDetailId("ee")
                        .identityMap(sensorsIdentities.getIdentityMap())
                        .addProperty("key1", "value1")
                        .addProperty("key2", new Date())
                        .start();
        sa.detailSet(detailSchema);
        assertSchemaDataNode(data);
        Object distinctId = ((Map<String, Object>) data.get("properties")).get("distinct_id");
        Assertions.assertEquals("user_key1+user_value1", distinctId);
    }

    /** 测试item实体明细数据生成逻辑 */
    @Test
    public void checkItemDetailSet() throws InvalidArgumentException {
        DetailSchema detailSchema =
                DetailSchema.init()
                        .setSchema("test")
                        .setDetailId("ee")
                        .setItemPair("item_key", "item_value")
                        .addProperty("key1", "value1")
                        .start();
        sa.detailSet(detailSchema);
        assertSchemaDataNode(data);
    }

    /** 手动指定 distinct_id,确认 distinct_id 是否为设置值 */
    @Test
    public void checkUserDistinctIdDetailSet() throws InvalidArgumentException {
        SensorsAnalyticsIdentity sensorsIdentities =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("user_key1", "user_value1")
                        .addIdentityProperty("user_key2", "user_value2")
                        .build();
        DetailSchema detailSchema =
                DetailSchema.init()
                        .setSchema("test")
                        .setDetailId("ee")
                        .identityMap(sensorsIdentities.getIdentityMap())
                        .setDistinctId("123")
                        .addProperty("key1", "value1")
                        .addProperty("key2", new Date())
                        .start();
        sa.detailSet(detailSchema);
        assertSchemaDataNode(data);
        Object distinctId = ((Map<String, Object>) data.get("properties")).get("distinct_id");
        Assertions.assertEquals("123", distinctId);
    }

    /** 不传入 identities,然后设置 distinct_id，最终数据不应该出现该值 */
    @Test
    public void checkDistinctIdDetailSet() throws InvalidArgumentException {
        DetailSchema detailSchema =
                DetailSchema.init()
                        .setSchema("test")
                        .setDetailId("ee")
                        .setDistinctId("123")
                        .addProperty("key1", "value1")
                        .addProperty("key2", new Date())
                        .start();
        sa.detailSet(detailSchema);
        assertSchemaDataNode(data);
        Assertions.assertFalse(
                ((Map<String, Object>) data.get("properties")).containsKey("distinct_id"));
    }

    /** 同时传入 identities 和 itemPair 会导致抛出参数不合法异常 */
    @Test
    public void checkIdentitiesAndItemIdDetailSet() {
        SensorsAnalyticsIdentity sensorsIdentities =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("user_key1", "user_value1")
                        .addIdentityProperty("user_key2", "user_value2")
                        .build();
        String message = "";
        try {
            DetailSchema detailSchema =
                    DetailSchema.init()
                            .setSchema("test")
                            .setDetailId("ee")
                            .identityMap(sensorsIdentities.getIdentityMap())
                            .setItemPair("item_key", "item_value")
                            .setDistinctId("123")
                            .addProperty("key1", "value1")
                            .addProperty("key2", new Date())
                            .start();
        } catch (InvalidArgumentException e) {
            message = e.getMessage();
        }
        Assertions.assertEquals("detail schema cannot both set identities and itemPair.", message);
    }

    @Test
    public void checkDetailDelete() throws InvalidArgumentException {
        DetailSchema detailSchema =
                DetailSchema.init()
                        .setSchema("test")
                        .setDetailId("ee")
                        .setItemPair("item_key", "item_value")
                        .setDistinctId("123")
                        .addProperty("key1", "value1")
                        .addProperty("key2", new Date())
                        .start();
        sa.detailDelete(detailSchema);
        assertSchemaDataNode(data);
    }
}
