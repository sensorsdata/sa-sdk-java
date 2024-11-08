package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.consumer.BatchConsumer;
import com.sensorsdata.analytics.javasdk.consumer.Callback;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import java.util.Arrays;
import org.apache.http.impl.client.HttpClients;
import org.junit.jupiter.api.Test;

public class InstantEventTest extends SensorsBaseTest {

    private static final String SCHEMA = "product_event";

    private static final String EVENT_NAME = "cart";

    public void initBatchConsumer() {
        BatchConsumer bc =
                new BatchConsumer(
                        "http://localhost:8888/instant",
                        50,
                        0,
                        true,
                        3,
                        Arrays.asList("test1", "test4"));
        sa = new SensorsAnalytics(bc);
        data = null;
        res.clear();
    }

    @Test
    public void checkBatchConsumer() throws InvalidArgumentException {
        initBatchConsumer();
        EventRecord event1 =
                EventRecord.builder()
                        .setEventName("test1")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event2 =
                EventRecord.builder()
                        .setEventName("test2")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event3 =
                EventRecord.builder()
                        .setEventName("test3")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event4 =
                EventRecord.builder()
                        .setEventName("test4")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event5 =
                EventRecord.builder()
                        .setEventName("test5")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();

        sa.track(event1);

        sa.flush();
        sa.track(event2);
        sa.track(event3);
        sa.track(event4);
        sa.track(event5);
        sa.flush();
    }

    public void initFastBatchConsumer() {
        FastBatchConsumer bc =
                new FastBatchConsumer(
                        HttpClients.custom(),
                        "http://localhost:8888/instant",
                        false,
                        50,
                        0,
                        3,
                        3,
                        new Callback() {
                            @Override
                            public void onFailed(FailedData failedData) {
                                SensorsLogsUtil.setFailedData(failedData);
                            }
                        },
                        Arrays.asList("test1", "test4"));
        sa = new SensorsAnalytics(bc);
        data = null;
        res.clear();
    }

    @Test
    public void checkFastBatchConsumer() throws InvalidArgumentException {
        initFastBatchConsumer();
        EventRecord event1 =
                EventRecord.builder()
                        .setEventName("test1")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event2 =
                EventRecord.builder()
                        .setEventName("test2")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event3 =
                EventRecord.builder()
                        .setEventName("test3")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event4 =
                EventRecord.builder()
                        .setEventName("test4")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();
        EventRecord event5 =
                EventRecord.builder()
                        .setEventName("test5")
                        .setDistinctId("1234")
                        .isLoginId(false)
                        .build();

        sa.track(event1);

        sa.flush();
        sa.track(event2);
        sa.track(event3);
        sa.track(event4);
        sa.track(event5);
        sa.flush();
    }
    //
    //  public void initRemoteBatchConsumer() {
    //    FastBatchConsumer bc = new FastBatchConsumer(HttpClients.custom(),
    // "http://10.120.213.104:8106/sa?project=default",
    //        false , 50, 0, 3, 3,  new Callback() {
    //      @Override
    //      public void onFailed(FailedData failedData) {
    //        SensorsLogsUtil.setFailedData(failedData);
    //      }
    //    }, Arrays.asList("JavaSDKInstantEvent"));
    //    sa = new SensorsAnalytics(bc);
    //    data = null;
    //    res.clear();
    //  }
    //
    //  @Test
    //  public void checkRemoteBatchConsumer() throws InvalidArgumentException {
    //    initRemoteBatchConsumer();
    //    EventRecord event1 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event2 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event3 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event4 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event5 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //
    //    sa.track(event1);
    //
    //    sa.flush();
    //    sa.track(event2);
    //    sa.track(event3);
    //    sa.track(event4);
    //    sa.track(event5);
    //    sa.flush();
    //
    //  }
    //
    //  public void initRemoteFastBatchConsumer() {
    //    FastBatchConsumer bc = new FastBatchConsumer(HttpClients.custom(),
    // "http://10.120.213.104:8106/sa?project=default",
    //        false , 50, 0, 3, 3,  new Callback() {
    //      @Override
    //      public void onFailed(FailedData failedData) {
    //        SensorsLogsUtil.setFailedData(failedData);
    //      }
    //    }, Arrays.asList("JavaSDKInstantEvent"));
    //    sa = new SensorsAnalytics(bc);
    //    data = null;
    //    res.clear();
    //  }
    //
    //  @Test
    //  public void checkRemoteFastBatchConsumer() throws InvalidArgumentException {
    //    initRemoteFastBatchConsumer();
    //    EventRecord event1 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event2 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event3 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event4 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //    EventRecord event5 =
    // EventRecord.builder().setEventName("JavaSDKInstantEvent").setDistinctId("o826p5zjQny5wKCAxRWss9bzWmlI").addProperty("myname", "qinglintest").isLoginId(false).build();
    //
    //    sa.track(event1);
    //
    //    sa.flush();
    //    sa.track(event2);
    //    sa.track(event3);
    //    sa.track(event4);
    //    sa.track(event5);
    //    sa.flush();
    //
    //  }
}
