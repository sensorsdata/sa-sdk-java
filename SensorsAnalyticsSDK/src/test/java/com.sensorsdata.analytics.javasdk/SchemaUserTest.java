package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * v3.4.5+ 用户 schema
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/06/16 18:12
 */
public class SchemaUserTest extends SensorsBaseTest {

    private static final Long USER_ID = 12345L;

    private static final String DISTINCT_ID = "fz123";
    /** 用户 */
    @Test
    public void checkProfileSetTest() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("user1", "value1")
                        .addProperty("key1", "value1")
                        .addProperty("key2", 22)
                        .setDistinctId("aaa")
                        .start();
        sa.profileSet(userSchema);
        assertUSData(data);
    }

    @Test
    public void checkProfileSetTest2() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("user1", Arrays.asList("value1", "value2"))
                        .addProperty("key1", "value1")
                        .addProperty("key2", 22)
                        .setDistinctId("aaa")
                        .start();
        sa.profileSet(userSchema);
        assertUSData(data);
    }

    @Test
    public void checkProfileSetOnce() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("login_id", DISTINCT_ID)
                        .addIdentityProperty("key1", "value1")
                        .setDistinctId("aaa")
                        .start();
        sa.profileSetOnce(userSchema);
        assertUSData(data);
    }

    @Test
    public void checkProfileSetIncrement() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("login_id", DISTINCT_ID)
                        .addProperty("key1", 20)
                        .start();
        sa.profileIncrement(userSchema);
        assertUSData(data);
    }

    @Test
    public void checkProfileAppend() throws InvalidArgumentException {
        List<String> others = new ArrayList<>();
        others.add("swim");
        others.add("run");
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("login_id", DISTINCT_ID)
                        .addProperty("key1", others)
                        .start();
        sa.profileAppend(userSchema);
        assertUSData(data);
    }

    @Test
    public void checkProfileUnset() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("login_id", DISTINCT_ID)
                        .addProperty("key1", true)
                        .start();
        sa.profileUnset(userSchema);
        assertUSData(data);
    }

    /** 删除用户属性；只支持传入单个用户 */
    @Test
    public void checkProfileDeleteByUserId() throws InvalidArgumentException {
        sa.profileDelete("key1", "value1");
        assertUSData(data);
    }

    @Test
    public void checkPreDefineProperties() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .addIdentityProperty("key1", "value1")
                        .addProperty("$project", "abc")
                        .start();
        sa.profileSet(userSchema);
        assertUSData(data);
    }

    // ----------------------------------v3.5.2--------------------------------------

    /**
     * 支持 user 数据传入 userId 作为用户标识
     *
     * <p>期望：用户数据传入 userId,最终节点中包含 userId 和 distinctId 信息
     */
    @Test
    public void checkUserId() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init().setUserId(123L).addProperty("$project", "abc").start();
        sa.profileSet(userSchema);
        assertUSData(data);
    }

    /**
     * 同时传入 userID 和 identities 节点
     *
     * <p>期望：userId 优先级最高，两者同时传入，最终数据中存在 userId
     */
    @Test
    public void checkUserIdAndIdentities() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .setUserId(123L)
                        .addIdentityProperty("key1", "value1")
                        .addProperty("$project", "abc")
                        .start();
        sa.profileSet(userSchema);
        assertUSData(data);
        Assertions.assertTrue(data.containsKey("id"));
    }

    /**
     * 同时传入 userID 和 distinctId 节点
     *
     * <p>期望：最终数据节点中，以传入的 distinctId 为主
     */
    @Test
    public void checkUserIdAndDistinctId() throws InvalidArgumentException {
        UserSchema userSchema =
                UserSchema.init()
                        .setUserId(123L)
                        .setDistinctId("test")
                        .addIdentityProperty("key1", "value1")
                        .addProperty("$project", "abc")
                        .start();
        sa.profileSet(userSchema);
        assertUSData(data);
        Assertions.assertTrue(data.containsKey("distinct_id"));
        Assertions.assertEquals("test", data.get("distinct_id"));
    }
}
