package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertNotNull;

import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.DebugConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *  适用于 v3.4.2+ 版本
 *  测试数据是否能正常上传
 */
public class IDMappingModel3TestDebugConsumer extends SensorsBaseTest {
    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    SensorsAnalytics saTmp;

    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {
//        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        String url = "http://10.120.111.143:8106/sa?project=default";
        DebugConsumer debugConsumer = new DebugConsumer(url, true);
        saTmp = new SensorsAnalytics(debugConsumer);


    }

    private void assertNotNullProp(){
        assertNotNull(messageList.get(0).get("identities"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertNotNull(messageList.get(0).get("properties"));
        assertNotNull(messageList.get(0).get("project"));
        assertNotNull(messageList.get(0).get("token"));
    }

    @Test
    public void testSingleIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        saTmp.trackById(identity, "test", properties);
    }

    @Test
    public void testTrackByIdMultiIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .build();
        Map<String, Object> properties = new HashMap<>();
        saTmp.trackById(identity, "testMultiIdentity", properties);

    }

    @Test
    public void testTrackByIdMultiIdentity02() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "13800000001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .addIdentityProperty("MyID", "001")
                .build();
        Map<String, Object> properties = new HashMap<>();

        saTmp.trackById(identity, "testMultiIdentity", properties);

    }


    /**
     * 校验 ID-Mapping bind 接口
     */
    @Test
    public void testIdMappingBind() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("$identity_mobile", "123")
                .addIdentityProperty("$identity_email", "fz@163.com")
                .build();
        saTmp.bind(identity);
    }


    /**
     * 校验 ID-Mapping unbind 接口用户格式
     */
    @Test
    public void checkUnbindUserId() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.unbind(identity);

    }

    /**
     * 校验 ID-Mapping 公共属性
     */
    @Test
    public void checkTrackByIdSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);
    }

    /**
     * 校验 ID-Mapping bind 接口公共属性
     */
    @Test
    public void checkBindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .addIdentityProperty("eee", "123")
                .build();
        saTmp.bind(identity);

    }

    /**
     * 校验 ID_Mapping unbind 接口公共属性
     */
    @Test
    public void checkUnbindSuperProperties() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("asd", "123");
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.unbind(identity);
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     */
    @Test
    public void testTrackByIdIDMEventRecord() throws InvalidArgumentException  {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .setDistinctId("disId123")
                .setEventName("test")
                .build();

        saTmp.trackById(record);
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 传入正常 distinctId，identityMap 传入其他用户标识
     */
    @Test
    public void testTrackByIdIDMEventRecord01() throws InvalidArgumentException {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .setDistinctId("disId123")
                .setEventName("test")
                .build();

        saTmp.trackById(record);
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 不传入 distinctId，identityMap 传入 $identity_login_id
     */

    @Test
    public void testTrackByIdIDMEventRecord02() throws InvalidArgumentException {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "xc001")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .setEventName("test")
                .build();

        saTmp.trackById(record);
    }

    /**
     *  v3.4.2 后新增接口 trackById(@NonNull IDMEventRecord idmEventRecord) 测试
     *  测试点：IDMEventRecord 不传入 distinctId，identityMap 传入其他用户标识
     */

    @Test
    public void testTrackByIdIDMEventRecord03() throws InvalidArgumentException {
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "1300000055")
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "email1@qq.com")
                .setEventName("test")
                .build();

        saTmp.trackById(record);
    }

    /**
     * 校验 ID-Mapping unbind 接口用户格式
     * 测试点：SensorsAnalyticsIdentity 不传入 $identity_login_id
     */
    @Test
    public void testUnbindUserId02() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .addIdentityProperty("$anonymous_id", "anonymous_id_value1")
                .addIdentityProperty("$login_id", "login_id_value1")
                .build();
        saTmp.unbind(identity);
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetById() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetById(identity, properties);
    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnesById00() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileSetOnceById(identity, properties);

    }

    /**
     * SensorsAnalyticsIdentity 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementById00() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("age", 1);
        saTmp.profileIncrementById(identity, properties);
    }

    @Test
    public void testProfileAppendById() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", list);
        saTmp.profileAppendById(identity, properties);
    }

    @Test
    public void testProfileUnsetById() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");
        Map<String, Object> properties = new HashMap<>();
        properties.put("favorite", true);
        saTmp.profileUnsetById(identity, properties);
    }

    // profileDeleteById
    @Test
    public void testProfileDeleteById() throws InvalidArgumentException {
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                .build();

        saTmp.profileDeleteById(identity);
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetByIdNew() throws InvalidArgumentException {
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetById(userRecord);
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileSetOnceByIdNew() throws InvalidArgumentException {
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileSetOnceById(userRecord);
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileIncrementByIdNew() throws InvalidArgumentException {
        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .build();
        saTmp.profileIncrementById(userRecord);
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileAppendByIdNew() throws InvalidArgumentException {
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", list) // 设置埋点事件属性
                .build();
        saTmp.profileAppendById(userRecord);
    }

    /**
     * IDMUserRecord 传入正常 distinctId，identityMap 传入 $identity_login_id
     * @throws InvalidArgumentException
     */
    @Test
    public void testProfileUnsetByIdNew() throws InvalidArgumentException {
        List<String> list = new ArrayList<>();
        list.add("apple");
        list.add("orange");

        // 新版本接口
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("favorite", true) // 设置埋点事件属性
                .build();
        saTmp.profileUnsetById(userRecord);
    }

}
