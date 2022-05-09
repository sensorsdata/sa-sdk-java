package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.DebugConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import org.junit.Before;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 *  适用于 v3.4.4+ 版本
 *  测试数据是否能正常上传
 */
public class IDMappingModel3TestTrackIdAndTimeDebugConsumer extends SensorsBaseTest {
    private BatchConsumer batchConsumer;

    private List<Map<String, Object>> messageList;
    SensorsAnalytics saTmp;

    @Before
    public void init() {
//        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        String url = "http://10.120.111.143:8106/sa?project=default";
//        String url = "http://10.120.101.188:8106/sa?project=default";
        DebugConsumer debugConsumer = new DebugConsumer(url, true);
        saTmp = new SensorsAnalytics(debugConsumer);
    }

    private void assertNotNullProp(){
        assertNotNull(messageList.get(0).get("identities"));
        assertNotNull(messageList.get(0).get("time"));
        assertNotNull(messageList.get(0).get("_track_id"));
        assertNotNull(messageList.get(0).get("properties"));
    }

    @Test
    public void testTrackSame() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test", properties);
        saTmp.trackById(identity, "test", properties);
    }

    @Test
    public void testTrackDiffTrackId() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test", properties);

        Map<String, Object> properties2 = new HashMap<>();
        properties2.put("$track_id", 222);
        properties2.put("$time", date);
        saTmp.trackById(identity, "test", properties2);
    }

    @Test
    public void testTrackDiffTime() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test", properties);

        Map<String, Object> properties2 = new HashMap<>();
        properties2.put("$track_id", 111);
        properties2.put("$time", new Date());
        saTmp.trackById(identity, "test", properties2);
    }

    @Test
    public void testTrackDiffEventName() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test111", properties);
        saTmp.trackById(identity, "test222", properties);
    }

    @Test
    public void testTrackDiffUserID() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        SensorsAnalyticsIdentity identity2 = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.MOBILE, "122255555")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test", properties);
        saTmp.trackById(identity2, "test", properties);
    }


    @Test
    public void testSingleIdentity() throws InvalidArgumentException{
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .build();
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

        saTmp.trackById(identity, "test", properties);
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
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("abc", 111);
        saTmp.registerSuperProperties(properties);
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                .addIdentityProperty("id_test1", "id_value1")
                .build();
        saTmp.trackById(identity, "eee", null);
    }

    /**
     * 校验 ID-Mapping 公共属性
     */
    @Test
    public void checkTrackByIdSuperProperties01() throws InvalidArgumentException {
        Map<String, Object> properties = new HashMap<>();
        properties.put("$track_id", "aaa");
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
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
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
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
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        properties.put("abc", 111);
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
        Date date = new Date();
        IDMEventRecord record = IDMEventRecord.starter()
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "123")
                .setDistinctId("disId123")
                .setEventName("test")
                .addProperty("$track_id", 111)
                .addProperty("$time", date)
                .addProperty("abc", 111)
                .build();

        saTmp.trackById(record);
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
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);

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
        properties.put("$track_id", 111);
        Date date = new Date();
        properties.put("$time", date);
        saTmp.profileSetOnceById(identity, properties);


}

    @Test
    public void testProfileUnsetById() {
        try {
            SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
                    .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123")
                    .build();
            List<String> list = new ArrayList<>();
            list.add("apple");
            list.add("orange");
            Map<String, Object> properties = new HashMap<>();
            properties.put("favorite", true);
            properties.put("$track_id", 111);
            Date date = new Date();
            properties.put("$time", date);

            saTmp.profileUnsetById(identity, properties);
            fail("profileUnsetById should throw InvalidArgumentException.");
        }catch(InvalidArgumentException e){
            String msg = "The property value of [$time] should be true.";
            assertEquals(msg, e.getMessage());
            e.printStackTrace();
        }
}

    // profileDeleteById
    // TODO
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
        Date date = new Date();
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111)
                .addProperty("$time", date) // 设置 $track_id
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
        Date date = new Date();
        IDMUserRecord userRecord = IDMUserRecord.starter()
                .setDistinctId("xc001") //手动指定外层 distinct_id
                .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, "dis123") //用户维度标识
                .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, "123@qq.com")   //用户维度标识
                .addProperty("age", 1) // 设置埋点事件属性
                .addProperty("$track_id", 111)
                .addProperty("$time", date) // 设置 $track_id
                .build();
        saTmp.profileSetOnceById(userRecord);


}



}
