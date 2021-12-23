package com.sensorsdata.analytics.javasdk;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.consumer.Callback;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;
import lombok.SneakyThrows;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.*;
import java.util.concurrent.LinkedBlockingQueue;

import static org.junit.Assert.*;

/**
 * FastBatchConsumer 单测
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/24 19:08
 */
public class FastBatchConsumerTest {

    SensorsAnalytics sa;

    private LinkedBlockingQueue<Map<String, Object>> buffer;

    private FailedData failedData;

    private Object hasFailed = new Object();

    private volatile List<FailedData> dataList = new ArrayList<>();

    private FastBatchConsumer consumer;


    class RunnableDemo implements Runnable {
        private Thread t;
        private String threadName;
        SensorsAnalytics sa;

        RunnableDemo( String name, SensorsAnalytics sa) {
            this.sa = sa;
            threadName = name;
            System.out.println("Creating " +  threadName );
        }

        @SneakyThrows
        public void run() {
            System.out.println("Running " +  threadName );
            sa.track("123", true, "test", null);
        }

        public void start () {
            System.out.println("Starting " +  threadName );
            if (t == null) {
                t = new Thread (this, threadName);
                t.start ();
            }
        }
    }


    @Before
    public void init() throws NoSuchFieldException, IllegalAccessException {

        consumer = new FastBatchConsumer("http://localhost:8887/test", new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);
    }


    @Test
    public void testResendData() throws InvalidArgumentException, InterruptedException, JsonProcessingException {

        // 初始化 SA
        sa = new SensorsAnalytics(consumer);

        // 重发给一个正确的 serverUrl
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumerRight = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        //初始化重发送接口
        SensorsLogsUtil.resend(consumerRight);
        sa.trackSignUp("123", "456");
        sa.track("123", true, "test", null);
        sa.flush();
        sa.track("123", true, "test", null);
        sa.flush();
        Thread.sleep(10000);
    }


    @Test
    public void testResendDataFail() throws InvalidArgumentException, InterruptedException, JsonProcessingException {
        // 初始化 SA
        sa = new SensorsAnalytics(consumer);
        //初始化重发送接口
        SensorsLogsUtil.resend(consumer);

        sa.track("123", true, "test", null);
        sa.flush();
        Thread.sleep(5000);
    }


    @Test
    public void testServerUrl() throws InvalidArgumentException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        url = "https://oceandata.debugbox.sensorsdata.cn/sa?project=ocean&token=schemaLimited-EJfb617f";
        FastBatchConsumer consumer = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });
        SensorsAnalytics sa = new SensorsAnalytics(consumer);
        //初始化重发送接口
        SensorsLogsUtil.resend(consumer);
        sa.track("123", true, "test", null);
        sa.flush();
        Thread.sleep(5000);
    }

    @Test
    public void testMaxCatchFailed() throws InvalidArgumentException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumer = new FastBatchConsumer(url,true,1000,6000,30*60,3, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });
        SensorsAnalytics sa = new SensorsAnalytics(consumer);
        //初始化重发送接口
        SensorsLogsUtil.resend(consumer);
        for(int i = 0; i < 6001; i ++) {
            sa.track("123", true, "test" + i, null);
        }
        Thread.sleep(5000);
    }
    @Test
    public void testServerUrlNull() throws InvalidArgumentException, InterruptedException {
        String url = null;
        try {
            FastBatchConsumer consumer = new FastBatchConsumer(url, new Callback() {
                @Override
                public void onFailed(FailedData failedData) {
                    SensorsLogsUtil.setFailedData(failedData);
                    System.out.println(failedData);
                }
            });
        } catch (Exception e) {
            assertEquals("serverUrl is marked non-null but is null", e.getMessage());
        }
    }

    @Test
    public void testServerUrlEmpty() throws InvalidArgumentException, InterruptedException, NoSuchFieldException, IllegalAccessException {
        String url = "";
        FastBatchConsumer consumer = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
                System.out.println(failedData);
                assertEquals("failed to send data,message:null.", failedData.getFailedMessage());
//        assertEquals("", failedData.getFailedData().get);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);
        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        sa.track("123", true, "test", null);
        sa.flush();
    }

    @Test
    public void testBulkSize() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        for (int i = 0; i < 49; i++){
            sa.track("123", true, "test" + i, null);
        }
        assertEquals(49,buffer.size());
        Thread.sleep(3000);
        sa.track("123", true, "test49", null);
        Thread.sleep(3000);
        assertEquals(0, buffer.size());
    }

    @Test
    public void testBulkSize100() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false,100, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        for (int i = 0; i < 99; i++){
            sa.track("123", true, "test" + i, null);
        }
        assertEquals(99,buffer.size());
        Thread.sleep(3000);
        sa.track("123", true, "test99", null);
        Thread.sleep(3000);
        assertEquals(0, buffer.size());
    }

    @Test
    public void testBulkSize1001() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false,1001, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(1000,bulkSize);
        for (int i = 0; i < 999; i++){
            sa.track("123", true, "test" + i, null);
        }
//        assertEquals(1000,buffer.size());
        Thread.sleep(5000);
        sa.track("123", true, "test999", null);
        Thread.sleep(5000);
        assertEquals(0, buffer.size());
    }

    @Test
    public void testBulkSizeInvalid() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false,0, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(1,bulkSize);
        sa.track("123", true, "test", null);
        Thread.sleep(2000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testBulkSizeInvalid1() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false,-1, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(1,bulkSize);
        sa.track("123", true, "test", null);
        Thread.sleep(2000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testBulkSizeInvalid2() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://localhost:8887/test";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false,-1, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(1,bulkSize);
        sa.track("123", true, "test", null);
        Thread.sleep(2000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testTiming() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://localhost:8887/test";

        FastBatchConsumer consumer = new FastBatchConsumer(url,true,1000, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(1000,bulkSize);
        sa.track("123", true, "test", null);
        Thread.sleep(1000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testFlushSec0() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://localhost:8887/test";

        FastBatchConsumer consumer = new FastBatchConsumer(url,0, true,new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(50,bulkSize);
        sa.track("123", true, "test", null);
        assertEquals(1,buffer.size());
        Thread.sleep(1000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testFlushSec1() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://localhost:8887/test";

        FastBatchConsumer consumer = new FastBatchConsumer(url,-1, true,new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(50,bulkSize);
        sa.track("123", true, "test", null);
        Thread.sleep(1000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testTimingFalse() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,4, false, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        assertEquals(50,bulkSize);
        sa.track("123", true, "test", null);
        Thread.sleep(4000);
        assertEquals(1,buffer.size());

        for(int i = 0; i < 49; i++){
            sa.track("123", true, "test"+i, null);
        }
        Thread.sleep(4000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testMaxCacheSize() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false, 50,8000, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                assertEquals("can't offer to buffer.", failedData.getFailedMessage());
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        for(int i = 0; i < 8001; i++){
            sa.track("123", true, "test"+i, null);
        }
        Thread.sleep(4000);
        assertEquals(0,buffer.size());
    }

    @Test
    public void testMaxCacheSize10001() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false, 50,10001, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                assertEquals("can't offer to buffer.", failedData.getFailedMessage());
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        for(int i = 0; i < 10001; i++){
            sa.track("123", true, "test"+i, null);
        }
        Thread.sleep(4000);
        assertEquals(0,buffer.size());
    }



    Boolean isFailed = false;
    @Test
    public void testMaxCacheSize999() throws InvalidArgumentException, NoSuchFieldException, IllegalAccessException, InterruptedException {
        String url = "http://10.120.73.51:8106/sa?project=default&token=";

        FastBatchConsumer consumer = new FastBatchConsumer(url,false, 50,999, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                isFailed = true;
                System.out.println(failedData);
            }
        });

        Field field = consumer.getClass().getDeclaredField("buffer");
        field.setAccessible(true);
        buffer = (LinkedBlockingQueue<Map<String, Object>>) field.get(consumer);

        field = consumer.getClass().getDeclaredField("bulkSize");
        field.setAccessible(true);
        int bulkSize = (Integer) field.get(consumer);

        SensorsAnalytics sa = new SensorsAnalytics(consumer);

        for(int i = 0; i < 999; i++){
            sa.track("123", true, "test"+i, null);
        }
        Thread.sleep(3000);
        assertFalse(isFailed);
        assertEquals(0,buffer.size());
    }

    /**
     *  需要使用 Charles 加断点
     */
    long endTime;
    @Test
    public void testTimeoutSec() throws InvalidArgumentException, InterruptedException, JsonProcessingException {
        String url = "http://localhost:8887/test";
        FastBatchConsumer consumer = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                endTime = System.currentTimeMillis();
                System.out.println("endTime: " + endTime);
                System.out.println(failedData);
            }
        });

        // 初始化 SA
        sa = new SensorsAnalytics(consumer);

        // 重发给一个正确的 serverUrl
        url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumer2 = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });

        //初始化重发送接口
        SensorsLogsUtil.resend(consumer2);

        sa.track("123", true, "test", null);
        long startTime = System.currentTimeMillis();
        System.out.println("startTime: " + startTime);
        sa.flush();
        Thread.sleep(4000);
        long eduration = endTime - startTime;
        System.out.println(eduration);
        assertTrue((eduration < 3300) && (eduration > 2300));
    }

    /**
     *  需要使用 Charles 加断点
     */
    @Test
    public void testTimeoutSec05() throws InvalidArgumentException, InterruptedException, JsonProcessingException {
        String url = "http://localhost:8887/test";
        FastBatchConsumer consumer = new FastBatchConsumer(url, false, 50, 1000,1, 5,new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                endTime = System.currentTimeMillis();
                System.out.println("endTime: " + endTime);
                System.out.println(failedData);
            }
        });

        // 初始化 SA
        sa = new SensorsAnalytics(consumer);

        // 重发给一个正确的 serverUrl
        url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumer2 = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });

        //初始化重发送接口
        SensorsLogsUtil.resend(consumer2);

        sa.track("123", true, "test", null);
        long startTime = System.currentTimeMillis();
        System.out.println("startTime: " + startTime);
        sa.flush();
        Thread.sleep(4000);
        long eduration = endTime - startTime;
        System.out.println(eduration);
        assertTrue((eduration < 5300) && (eduration > 4700));
    }

    /**
     *  需要使用 Charles 加断点
     */
    @Test
    public void testTimeoutSec01() throws InvalidArgumentException, InterruptedException, JsonProcessingException {
        String url = "http://localhost:8887/test";
        FastBatchConsumer consumer = new FastBatchConsumer(url, false, 50, 1000,1, 0,new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                endTime = System.currentTimeMillis();
                System.out.println("endTime: " + endTime);
                System.out.println(failedData);
            }
        });

        // 初始化 SA
        sa = new SensorsAnalytics(consumer);

        // 重发给一个正确的 serverUrl
        url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumer2 = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });

        //初始化重发送接口
        SensorsLogsUtil.resend(consumer2);

        sa.track("123", true, "test", null);
        long startTime = System.currentTimeMillis();
        System.out.println("startTime: " + startTime);
        sa.flush();
        Thread.sleep(4000);
        long eduration = endTime - startTime;
        System.out.println(eduration);
        assertTrue((eduration < 1300) && (eduration > 700));
    }

    @Test
    public void testHighlySendDataSuccess() throws InvalidArgumentException, InterruptedException, JsonProcessingException {
        // 初始化 SA


        // 重发给一个正确的 serverUrl
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumer2 = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });
        sa = new SensorsAnalytics(consumer2);
        //初始化重发送接口
        SensorsLogsUtil.resend(consumer2);

        for(int i = 0; i< 200; i++){
            RunnableDemo demo = new RunnableDemo("thread" + i, sa);
            demo.start();
        }
        Thread.sleep(30000);
    }

    @Test
    public void testHighlyResendData() throws InvalidArgumentException, InterruptedException, JsonProcessingException {
        // 初始化 SA
        sa = new SensorsAnalytics(consumer);

        // 重发给一个正确的 serverUrl
        String url = "http://10.120.73.51:8106/sa?project=default&token=";
        FastBatchConsumer consumer2 = new FastBatchConsumer(url, new Callback() {
            @Override
            public void onFailed(FailedData failedData) {
                SensorsLogsUtil.setFailedData(failedData);
            }
        });

        //初始化重发送接口
        SensorsLogsUtil.resend(consumer2);

        for(int i = 0; i< 200; i++){
            RunnableDemo demo = new RunnableDemo("thread" + i, sa);
            demo.start();
        }
        Thread.sleep(30000);
    }

    /**
     * 调用 send 接口，数据是否保存到缓存中
     */
    @Test
    public void checkSendData() {
        assertEquals(0, buffer.size());
        Map<String, Object> event = new HashMap<>();
        event.put("distinct_id", "12345");
        event.put("event", "test");
        event.put("type", "track");
        consumer.send(event);
        assertEquals(1, buffer.size());
        Map<String, Object> poll = buffer.poll();
        assertNotNull(poll);
        assertEquals(3, poll.size());
    }

    /**
     * 调用 flush 接口，设置错误的 URL，检查数据是否通过回调函数返回
     */
    @Test
    public void checkFlushWithErrorUrl() {
        assertEquals(0, buffer.size());
        Map<String, Object> event = new HashMap<>();
        event.put("distinct_id", "12345");
        event.put("event", "test");
        event.put("type", "track");
        consumer.send(event);
        consumer.flush();
        assertEquals(0, buffer.size());
    }

    /**
     * 重发送接口，设置错误的 URL，返回发送失败标志
     */
    @Test
    public void checkResendFailedDataWithErrorUrl() throws InvalidArgumentException, JsonProcessingException {
        Map<String, Object> event = new HashMap<>();
        event.put("_track_id", 123456789);
        event.put("distinct_id", "12345");
        event.put("event", "test");
        event.put("type", "track");
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        FailedData failedData = new FailedData("", list);
        boolean sendFlag = consumer.resendFailedData(failedData);
        assertFalse(sendFlag);
    }

    @Test
    public void checkResendFailedDataWithRightUrl() throws InvalidArgumentException, JsonProcessingException {
        final FastBatchConsumer fastBatchConsumer =
                new FastBatchConsumer("http://10.129.138.189:8106/sa?project=production", new Callback() {
                    @Override
                    public void onFailed(FailedData failedData) {
                        System.out.println(failedData);
                    }
                });
        Map<String, Object> event = new HashMap<>();
        event.put("_track_id", new Random().nextInt());
        event.put("distinct_id", "123456");
        event.put("event", "test");
        event.put("type", "track");
        ArrayList<Map<String, Object>> list = new ArrayList<>();
        list.add(event);
        boolean sendFlag = fastBatchConsumer.resendFailedData(new FailedData("", list));
        assertTrue(sendFlag);
    }
}
