package com.sensorsdata.analytics.javasdk.consumer;

import java.util.Map;

public interface Consumer {
    void send(Map<String, Object> message);

    void flush();

    void close();
}
