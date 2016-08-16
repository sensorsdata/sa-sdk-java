package com.sensorsdata.analytics.javasdk;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

public class HelloSensorsAnalytics {

  public static void main(String[] args) throws Exception {
    // Sensors Analytics 采集事件的 URL
    final String SA_SERVER_URL = "http://${service_name}.cloud.sensorsdata.cn:8006/sa?token=${token}&project=${project_name}";

    // DebugConsumer
    final SensorsAnalytics sa = new SensorsAnalytics(new SensorsAnalytics.DebugConsumer
        (SA_SERVER_URL, true));

    // BatchConsumer
//    final SensorsAnalytics sa =
//        new SensorsAnalytics(new SensorsAnalytics.BatchConsumer(SA_SERVER_URL, 10));

    // AsyncBatchConsumer
//    ThreadPoolExecutor executor =
//        new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>(),
//            new ThreadFactory() {
//              public Thread newThread(Runnable r) {
//                Thread t = new Thread(r);
//                t.setPriority(Thread.MIN_PRIORITY);
//                return t;
//              }
//            });
//    SensorsAnalytics.AsyncBatchConsumerCallback callback =
//        new SensorsAnalytics.AsyncBatchConsumerCallback() {
//          @Override public void onFlushTask(Future<Boolean> task) {
//            try {
//              task.get();
//            } catch (InterruptedException e) {
//              e.printStackTrace();
//            } catch (ExecutionException e) {
//              e.printStackTrace();
//            }
//          }
//        };
//    final SensorsAnalytics sa = new SensorsAnalytics(
//        new SensorsAnalytics.AsyncBatchConsumer(SA_SERVER_URL, 10, executor, callback));

    // ConsoleConsumer
//    Writer writer = new PrintWriter(System.out);
//    final SensorsAnalytics sa = new SensorsAnalytics(new SensorsAnalytics.ConsoleConsumer(writer));
//    writer.flush();

    // 在这个 Demo 中，我们以一个典型的电商产品为例，描述一个用户从匿名访问网站，到下单购买商品，再到申请售后服务，
    // 这样一个整个环节，使用 Sensors Analytics（以下简称 SA）的产品，应该如何记录日志。

    // 特别需要注意的是，这个 Demo 只是描述 SA 的数据记录能力，并不是说使用者要完全照搬这些 Event 和 Property
    // 的设计，使用者还是需要结合自己产品的实际需要，来做相应的设计和规划

    // 1. 用户匿名访问网站
    String cookieId = "ABCDEF123456789"; // 用户未登录时，可以使用产品自己生成的cookieId来标注用户
    Map<String, Object> properties = new HashMap<String, Object>();
    // 1.1 访问首页

    // 前面有$开头的property字段，是SA提供给用户的预置字段
    // 对于预置字段，已经确定好了字段类型和字段的显示名
    properties.clear();
    properties.put("$time", new Date());                // 这条event发生的时间，如果不设置的话，则默认是当前时间
    properties.put("$os", "Windows");                   // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");               // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");           // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("Channel", "baidu");                 // 用户是通过baidu这个渠道过来的
    sa.track(cookieId, "ViewHomePage", properties); // 记录访问首页这个event

    // 1.2 搜索商品
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");         // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("KeyWord", "XX手机");                // 搜索引擎引流过来时使用的关键词
    sa.track(cookieId, "SearchProduct", properties);      // 记录搜索商品这个event

    // 1.3 浏览商品
    properties.clear();
    properties.put("$os", "Windows");                 // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");             // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");         // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("ProductName", "xx手机");           // 商品名称
    properties.put("ProductType", "智能手机");         //  商品类别
    properties.put("ShopName", "XX官方旗舰店");         // 店铺名称
    sa.track(cookieId, "ViewProduct", properties);      // 记录浏览商品这个event

    // 2. 用户决定注册了
    String registerId = "123456";       // 用户注册时，分配给用户的注册Id

    // 2.1 通过，trackSignUP，把匿名ID和注册ID贯通起来
    properties.clear();
    properties.put("RegisterChannel", "baidu");   // 用户的注册渠道
    sa.trackSignUp(registerId, cookieId);

    // 2.2 用户注册时，填充了一些个人信息，可以用Profile接口记录下来
    Map<String, Object> profiles = new HashMap<String, Object>();
    profiles.put("$city", "武汉");      // 用户所在城市
    profiles.put("$province", "湖北");  // 用户所在省份
    profiles.put("$name", "昵称123");   // 用户的昵称
    profiles.put("$signup_time", new Date()); // 注册时间
    profiles.put("Gender", "male");     // 用户的性别
    profiles.put("Birthday", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse(
        "1984-11-03 " + "00:00:00"));                 // 用户的出生日期，特别注意，这个地方填入年龄是不合适的，因为年龄会随着时间而变化
    profiles.put("RegisterChannel", "baidu");  // 用户的注册渠道
    sa.profileSet(registerId, profiles);    // 此时传入的是注册ID了

    // 2.3 立刻刷新一下，让数据传到SA中
    sa.flush();

    // 3. 用户注册后，进行后续行为
    // 3.1 提交订单和提交订单详情
    // 这个订单里面包含一个手机和两个手机膜
    // 订单的信息
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");         // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");   // 订单ID
    properties.put("ShipPrice", 10.0);             // 运费
    properties.put("OrderTotalPrice", 1234.0);         // 订单的总价格，默认是元
    sa.track(registerId, "SubmitOrder", properties); // 注意，此时使用的已经是注册ID了
    // 订单中手机这个商品的信息
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "XX手机");    // 商品名称
    properties.put("ProductType", "智能手机");   // 商品类别
    properties.put("ShopName", "XX官方旗舰店");  // 店铺名称
    properties.put("ProductUnitPrice", 1200.0);   // 商品单价
    properties.put("ProductAmount", 1.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 1200.0);  // 商品总价
    sa.track(registerId, "SubmitOrderDetail", properties);
    // 订单中手机膜这个商品的信息
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "5寸钢化膜");    // 商品名称
    properties.put("ProductType", "手机配件");   // 商品类别
    properties.put("ShopName", "XX手机外设店");  // 店铺名称
    properties.put("ProductUnitPrice", 12.0);   // 商品单价
    properties.put("ProductAmount", 2.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 24.0);  // 商品总价
    sa.track(registerId, "SubmitOrderDetail", properties);

    // 3.2 支付订单和支付订单详情
    // 整个订单的支付情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");         // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");   // 订单ID
    properties.put("ShipPrice", 10.0);             // 运费
    properties.put("OrderTotalPrice", 1234.0);         // 订单的总价格，默认是元
    properties.put("PaymentMethod", "AliPay");              // 支付方式
    properties.put("AllowanceAmount", 30.0);                   // 补贴金额
    properties.put("AllowanceType", "首次下单红包");        // 补贴类型
    properties.put("PaymentAmount", 1204.0);      // 实际支付的订单金额
    sa.track(registerId, "PayOrder", properties);

    // 手机这个商品的支付情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "XX手机");    // 商品名称
    properties.put("ProductType", "智能手机");   // 商品类别
    properties.put("ShopName", "XX官方旗舰店");  // 店铺名称
    properties.put("ProductUnitPrice", 1200.0);   // 商品单价
    properties.put("ProductAmount", 1.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 1200.0);  // 商品总价
    properties.put("ProductAllowanceAmount", 30.0); // 假设这个补贴是在手机上的折扣
    properties.put("ProductAllowanceType", "首次下单红包");        // 补贴类型
    properties.put("ProductPaymentAmount", 1170.0); // 手机实际支付了这么多
    properties.put("PaymentMethod", "AliPay");      // 与订单保持一致
    sa.track(registerId, "PayOrderDetail", properties);

    // 手机膜这个商品的支付情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "5寸钢化膜");    // 商品名称
    properties.put("ProductType", "手机配件");   // 商品类别
    properties.put("ShopName", "XX手机外设店");  // 店铺名称
    properties.put("ProductUnitPrice", 12.0);   // 商品单价
    properties.put("ProductAmount", 2.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 24.0);  // 商品总价
    properties.put("ProductAllowanceAmount", 0.0); // 手机膜上并没有补贴
    properties.put("ProductAllowanceType", "无补贴");        // 补贴类型
    properties.put("ProductPaymentAmount", 24.0); // 手机膜实际支付了这么多
    properties.put("PaymentMethod", "AliPay");      // 与订单保持一致
    sa.track(registerId, "PayOrderDetail", properties);

    // 3.3 假设这个用户支付后反悔了，要取消订单
    // 整个订单的取消情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");         // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");   // 订单ID
    properties.put("ShipPrice", 10.0);             // 运费
    properties.put("OrderTotalPrice", 1234.0);         // 订单的总价格，默认是元
    properties.put("CancelReason", "地址填写错误"); // 取消订单的原因
    properties.put("CancelTiming", "AfterPay");   // 取消订单的时机
    sa.track(registerId, "CancelOrder", properties);
    // 手机这个商品的取消情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "XX手机");    // 商品名称
    properties.put("ProductType", "智能手机");   // 商品类别
    properties.put("ShopName", "XX官方旗舰店");  // 店铺名称
    properties.put("ProductUnitPrice", 1200.0);   // 商品单价
    properties.put("ProductAmount", 1.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 1200.0);  // 商品总价
    properties.put("CancelReason", "地址填写错误"); // 取消订单的原因
    properties.put("CancelTiming", "AfterPay");   // 取消订单的时机
    sa.track(registerId, "CancelOrderDetail", properties);
    // 手机膜这个商品的取消情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "5寸钢化膜");    // 商品名称
    properties.put("ProductType", "手机配件");   // 商品类别
    properties.put("ShopName", "XX手机外设店");  // 店铺名称
    properties.put("ProductUnitPrice", 12.0);   // 商品单价
    properties.put("ProductAmount", 2.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 24.0);  // 商品总价
    properties.put("CancelReason", "地址填写错误"); // 取消订单的原因
    properties.put("CancelTiming", "AfterPay");   // 取消订单的时机
    sa.track(registerId, "CancelOrderDetail", properties);

    // 4. 假设用户并没有取消订单，然后成功完成了配送
    // 手机这个商品的配送情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "XX手机");    // 商品名称
    properties.put("ProductType", "智能手机");   // 商品类别
    properties.put("ShopName", "XX官方旗舰店");  // 店铺名称
    properties.put("ProductUnitPrice", 1200.0);   // 商品单价
    properties.put("ProductAmount", 1.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 1200.0);  // 商品总价
    properties.put("ProductAllowanceAmount", 30.0); // 假设这个补贴是在手机上的折扣
    properties.put("ProductPaymentAmount", 1170.0); // 手机实际支付了这么多
    properties.put("PaymentMethod", "AliPay");      // 与订单保持一致
    properties.put("SupplyTime", 49.0);       // 订单运送用了49小时
    properties.put("SupplyMethod", "顺丰");   // 通过顺丰运送的
    sa.track(registerId, "ReceiveProduct", properties);
    // 手机膜这个商品的配送情况
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "5寸钢化膜");    // 商品名称
    properties.put("ProductType", "手机配件");   // 商品类别
    properties.put("ShopName", "XX手机外设店");  // 店铺名称
    properties.put("ProductUnitPrice", 12.0);   // 商品单价
    properties.put("ProductAmount", 2.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 24.0);  // 商品总价
    properties.put("ProductAllowanceAmount", 0.0); // 手机膜上并没有补贴
    properties.put("ProductPaymentAmount", 24.0); // 手机膜实际支付了这么多
    properties.put("PaymentMethod", "AliPay");      // 与订单保持一致
    properties.put("SupplyTime", 98.0);       // 订单运送用了98小时
    properties.put("SupplyMethod", "圆通");   // 通过顺丰运送的
    sa.track(registerId, "ReceiveProduct", properties);


    // 5. 假设产品有质量问题，用户申请了售后服务
    // 5.1 手机申请了维修
    // 提交了售后服务
    properties.clear();
    properties.put("$os", "Windows");         // 通过请求中的UA，可以解析出用户使用设备的操作系统是windows的
    properties.put("$os_version", "8.1");     // 操作系统的具体版本
    properties.put("$ip", "123.123.123.123");     // 请求中能够拿到用户的IP，则把这个传递给SA，SA会自动根据这个解析省份、城市
    properties.put("OrderId", "SN_123_AB_TEST");  // 订单ID
    properties.put("ProductName", "XX手机");    // 商品名称
    properties.put("ProductType", "智能手机");   // 商品类别
    properties.put("ShopName", "XX官方旗舰店");  // 店铺名称
    properties.put("ProductUnitPrice", 1200.0);   // 商品单价
    properties.put("ProductAmount", 1.0);         // 商品数量，可以是个数，也可以是重量
    properties.put("ProductTotalPrice", 1200.0);  // 商品总价
    properties.put("ProductAllowanceAmount", 30.0); // 假设这个补贴是在手机上的折扣
    properties.put("ProductPaymentAmount", 1170.0); // 手机实际支付了这么多
    properties.put("PaymentMethod", "AliPay");    // 与订单保持一致
    properties.put("ServiceContent", "维修");     // 手机申请了维修
    properties.put("ServiceStatus", "新提交");     // 售后服务的状态
    sa.track(registerId, "ServiceAfterSale", properties);
    // 产品被收到，开始维修了，记录一条新的数据
    properties.put("ServiceStatus", "维修中");     // 售后服务的状态
    sa.track(registerId, "ServiceAfterSale", properties);
    // 维修完成了，再记录一条新的数据
    properties.put("ServiceStatus", "维修完成");   // 售后服务的状态
    sa.track(registerId, "ServiceAfterSale", properties);

    sa.shutdown();   // 关闭API，关闭时会自动调用flush
  }

}
