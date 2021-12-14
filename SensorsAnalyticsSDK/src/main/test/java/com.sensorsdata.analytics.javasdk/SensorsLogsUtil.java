package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.FailedData;
import com.sensorsdata.analytics.javasdk.consumer.FastBatchConsumer;
import lombok.SneakyThrows;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class SensorsLogsUtil {

    //定义一个定时线程，用于定时检查容器中是否存在失败数据
    private static final ScheduledExecutorService resendService = Executors.newSingleThreadScheduledExecutor();

    //失败数据容器，（此处用于模式测试，生产中可能会有大量数据，因此不建议使用内存容器，建议持久化）
    private static final List<FailedData> failedDataList = new ArrayList<>();

    //往容器中保存数据
    public static void setFailedData(FailedData failedData) {
        failedDataList.add(failedData);
        for (FailedData fd : failedDataList) {
            System.out.println("failed data." + fd);
            System.out.println("failed data." + fd.getFailedData());
        }
    }

    //初始化重发送操作
    public static void resend(FastBatchConsumer consumer) {
        resendService.scheduleWithFixedDelay(new ResendTask(consumer), 0, 1, TimeUnit.SECONDS);
    }

    static class ResendTask implements Runnable {

        private final FastBatchConsumer consumer;

        public ResendTask(FastBatchConsumer consumer) {
            this.consumer = consumer;
        }

        @SneakyThrows
        @Override
        public void run() {
            System.out.println("start schedule task.");
            if (!failedDataList.isEmpty()) {
                for (FailedData failedData : failedDataList) {
                    System.out.println("resend failed data." + failedData);
                    System.out.println("resend failed data." + failedData.getFailedData());
                    boolean isSend = consumer.resendFailedData(failedData);
                    System.out.println("resend success:" + isSend);
                    if(isSend){
                        failedDataList.remove(failedData);
                    }
                }
            }
        }
    }
}


