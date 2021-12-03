package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.*;
import com.sensorsdata.analytics.javasdk.consumer.ConcurrentLoggingConsumer;

import java.util.*;

/**
 * @author fz
 * @version 1.0.0
 * @since 2021/05/24 15:31
 */
public class HelloSensorsAnalytics {


    public static void main(final String[] args) throws Exception {
        // LoggingConsumer
        final ISensorsAnalytics sa = new SensorsAnalytics(new ConcurrentLoggingConsumer("file.log"));

        //设置公共属性,以后上传的每一个事件都附带该属性
        SuperPropertiesRecord propertiesRecord = SuperPropertiesRecord.builder()
                                                     .addProperty("$os", "Windows")
                                                     .addProperty("$os_version", "8.1")
                                                     .addProperty("$ip", "123.123.123.123")
                                                     .build();
        sa.registerSuperProperties(propertiesRecord);

        // 1. 用户匿名访问网站，cookieId 默认神策生成分配
        String cookieId = "ABCDEF123456789";
        // 1.1 访问首页
        // 前面有$开头的property字段，是SA提供给用户的预置字段
        // 对于预置字段，已经确定好了字段类型和字段的显示名
        EventRecord firstRecord = EventRecord.builder().setDistinctId(cookieId).isLoginId(Boolean.FALSE)
                                      .setEventName("track")
                                      .addProperty("$time", Calendar.getInstance().getTime())
                                      .addProperty("Channel", "baidu")
                                      .addProperty("$project", "abc")
                                      .addProperty("$token", "123")
                                      .build();
        sa.track(firstRecord);
        // 1.2 搜索商品
        EventRecord searchRecord = EventRecord.builder().setDistinctId(cookieId).isLoginId(Boolean.FALSE)
                                       .setEventName("SearchProduct")
                                       .addProperty("KeyWord", "XX手机")
                                       .build();
        sa.track(searchRecord);
        // 1.3 浏览商品
        EventRecord lookRecord = EventRecord.builder().setDistinctId(cookieId).isLoginId(Boolean.FALSE)
                                     .setEventName("ViewProduct")
                                     .addProperty("ProductName", "XX手机")
                                     .addProperty("ProductType", "智能手机")
                                     .addProperty("ShopName", "XX官方旗舰店")
                                     .build();
        sa.track(lookRecord);
        // 2. 用户注册登录之后，系统分配的注册ID
        String registerId = "123456";
        //使用trackSignUp关联用户匿名ID和登录ID
        sa.trackSignUp(registerId, cookieId);

        // 2.2 用户注册时，填充了一些个人信息，可以用Profile接口记录下来
        List<String> interests = new ArrayList<String>();
        interests.add("movie");
        interests.add("swim");
        UserRecord userRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                    .addProperty("$city", "武汉")
                                    .addProperty("$province", "湖北")
                                    .addProperty("$name", "昵称123")
                                    .addProperty("$signup_time", Calendar.getInstance().getTime())
                                    .addProperty("Gender", "male")
                                    .addProperty("age", 20)
                                    .addProperty("interest", interests)
                                    .build();
        sa.profileSet(userRecord);

        //2.3 设置首次访问时间
        UserRecord firstVisitRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                          .addProperty("$first_visit_time", Calendar.getInstance().getTime())
                                          .build();
        sa.profileSetOnce(firstVisitRecord);

        //2.4 追加属性
        List<String> newInterest = new ArrayList<String>();
        newInterest.add("ball");
        UserRecord appendRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                      .addProperty("interest", newInterest)
                                      .build();
        sa.profileAppend(appendRecord);

        //2.5 给属性加值
        UserRecord incrementRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                         .addProperty("age", 2)
                                         .build();
        sa.profileIncrement(incrementRecord);

        //2.6 移除用户属性
        UserRecord unsetRecord = UserRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                     .addProperty("age", 1)
                                     .build();
        sa.profileUnset(unsetRecord);

        // 3. 用户注册后，进行后续行为
        // 3.1 提交订单和提交订单详情
        // 订单的信息
        EventRecord orderRecord = EventRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                      .setEventName("SubmitOrder")
                                      .addProperty("OrderId", "SN_123_AB_TEST")
                                      .build();
        sa.track(orderRecord);

        // 3.2 支付订单和支付订单详情
        // 整个订单的支付情况
        EventRecord payRecord = EventRecord.builder().setDistinctId(registerId).isLoginId(Boolean.TRUE)
                                    .setEventName("PayOrder")
                                    .addProperty("PaymentMethod", "AliPay")
                                    .addProperty("AllowanceAmount", 30.0)
                                    .addProperty("PaymentAmount", 1204.0)
                                    .build();
        sa.track(payRecord);

        //物品纬度表上报
        String itemId = "product001", itemType = "mobile";
        ItemRecord addRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemType)
            .addProperty("color", "white")
            .build();
        sa.itemSet(addRecord);

        //删除物品纬度信息
        ItemRecord deleteRecord = ItemRecord.builder().setItemId(itemId).setItemType(itemType)
            .build();
        sa.itemDelete(deleteRecord);

        //Id-mapping
        SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty("email", "fz@163.com")
            .build();
        Map<String, Object> properties = new HashMap<String, Object>();
        properties.put("Channel", "baidu");
        sa.trackById(identity, "test", properties);
        SensorsAnalyticsIdentity identity1 = SensorsAnalyticsIdentity.builder()
            .addIdentityProperty("login_id", "fz123")
            .build();
        sa.bind(identity, identity1);
        sa.profileSetById(identity, "age", 15);
        List<String> sports = new ArrayList<String>();
        sports.add("swim");
        sports.add("run");
        Map<String, Object> hh = new HashMap<String, Object>();
        hh.put("sport", sports);
        sa.profileSetById(identity, hh);
        sa.profileIncrementById(identity, "age", 1);
        sa.profileAppendById(identity, "sport", "ball");
        sa.profileUnsetById(identity,"sport");
        sa.profileDeleteById(identity);
    }
}
