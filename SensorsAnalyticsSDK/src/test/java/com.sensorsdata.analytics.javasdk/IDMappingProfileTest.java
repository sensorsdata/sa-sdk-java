package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.ArrayList;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

/**
 * id-mapping profile 相关接口单元测试 版本支持 3.3.0+
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/03/12 11:10
 */
public class IDMappingProfileTest extends SensorsBaseTest {

    private final String loginId = "fz123";
    private final String email = "fz@163.com";

    // ------------------------------3.3.0----------------------------

    /** 用户使用 IDMUserRecord 调用 profileSetById 接口； 期望生成的数据结构符合神策数据格式 */
    @Test
    public void checkProfileSetById() throws InvalidArgumentException {
        final SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .build();
        sa.profileSetById(identity, "test", "ddd");
        assertIDM3UserData(data);
    }

    /** 用户生成 IDM3.0 profile 数据的时候，不需要生成 super 里面的属性 期望 properties 不产生 super 属性 */
    @Test
    public void checkProfileSuperProperties() throws InvalidArgumentException {
        final SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .build();
        sa.profileSetById(identity, "test", "ddd");
        assertIDM3UserData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertEquals(2, properties.size());
        Assertions.assertTrue(properties.containsKey("$is_login_id"));
        Assertions.assertTrue(properties.containsKey("test"));
    }

    // ------------------------------3.4.2----------------------------

    /** 用户生成 IDM3.0 profile 数据的时候，不需要生成 super 里面的属性 期望 properties 不产生 super 属性 */
    @Test
    public void checkProfileSuperProperties01() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "fz@163.com")
                        .addProperty("test", "ddd")
                        .build();
        sa.profileSetById(userRecord);
        assertIDM3UserData(data);
        Map<String, Object> properties = (Map<String, Object>) data.get("properties");
        Assertions.assertEquals(1, properties.size());
        Assertions.assertFalse(properties.containsKey("$is_login_id"));
        Assertions.assertTrue(properties.containsKey("test"));
    }

    /** 用户使用 IDMUserRecord 调用 profileSetById 接口； 期望生成的数据结构符合神策数据格式 */
    @Test
    public void checkProfileSetById01() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .addProperty("hello", "dd")
                        .build();
        sa.profileSetById(userRecord);
        assertIDM3UserData(data);
    }

    /**
     * 用户携带 $identity_login_id 使用 IDMUserRecord 调用 profileSetById 接口； 期望生成的数据结构，distinct_id 取值为
     * $identity_login_id
     */
    @Test
    public void checkProfileSetByIdWithLoginId() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .addProperty("hello", "dd")
                        .build();
        sa.profileSetById(userRecord);
        Assertions.assertEquals(loginId, data.get("distinct_id"));
    }

    /**
     * 用户携带 $identity_login_id，设置 distinct_id 使用 IDMUserRecord 调用 profileSetById 接口；
     * 期望生成的数据结构，distinct_id 取值为 $identity_login_id
     */
    @Test
    public void checkProfileSetByIdWithLoginId01() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .setDistinctId("zzz")
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .addProperty("hello", "dd")
                        .build();
        sa.profileSetById(userRecord);
        Assertions.assertEquals("zzz", data.get("distinct_id"));
    }

    /** 用户使用 IDMUserRecord 调用 profileSetOnceById 接口； 期望生成的数据结构符合神策数据格式 */
    @Test
    public void checkProfileSetOnceById() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .addProperty("hello", "dd")
                        .build();
        sa.profileSetOnceById(userRecord);
        assertIDM3UserData(data);
    }

    /** 用户使用 IDMUserRecord 调用 profileIncrementById 接口； 期望生成的数据结构符合神策数据格式 */
    @Test
    public void checkProfileIncrementById() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .addProperty("num", 33)
                        .build();
        sa.profileIncrementById(userRecord);
        assertIDM3UserData(data);
    }

    /** 用户使用 IDMUserRecord 调用 profileIncrementById 接口； 期望生成的数据结构符合神策数据格式 */
    @Test
    public void checkProfileAppendById() throws InvalidArgumentException {
        ArrayList<String> strings = new ArrayList<>();
        strings.add("movie");
        strings.add("swim");
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .addProperty("lists", strings)
                        .build();
        sa.profileAppendById(userRecord);
        assertIDM3UserData(data);
    }

    /** 用户使用 IDMUserRecord 调用 profileUnsetById 接口； 期望生成的数据结构符合神策数据格式 */
    @Test
    public void checkProfileUnsetById() throws InvalidArgumentException {
        IDMUserRecord userRecord =
                IDMUserRecord.starter()
                        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
                        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
                        .build();
        sa.profileUnsetById(userRecord);
        assertIDM3UserData(data);
    }

    // ------------------------------------bug fix 3.5.0 begin-------------------------------

    /**
     * bug fix : 修复解绑可以传入多个用户ID
     *
     * <p>修复后的逻辑为：解绑只可以传入一个用户维度ID
     */
    @Test
    public void checkUnbind() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity =
                SensorsAnalyticsIdentity.builder()
                        .addIdentityProperty("key1", "value1")
                        .addIdentityProperty("key2", "value2")
                        .build();
        try {
            sa.unbind(identity);
            Assertions.fail("生成异常数据！");
        } catch (InvalidArgumentException e) {
            Assertions.assertTrue(
                    e.getMessage()
                            .contains(
                                    "unbind user operation cannot input multiple or none identifiers"));
        }

        // 正常单个用户标识
        sa.unbind("key1", "value1");
        assertIDM3EventData(data);

        // 无用户标识
        SensorsAnalyticsIdentity noIdentity = SensorsAnalyticsIdentity.builder().build();
        try {
            sa.unbind(noIdentity);
            Assertions.fail("生成异常数据！");
        } catch (Exception e) {
            Assertions.assertTrue(
                    e.getMessage()
                            .contains(
                                    "unbind user operation cannot input multiple or none identifiers"));
        }
    }
    // ------------------------------------bug fix 3.5.0 end-------------------------------
}
