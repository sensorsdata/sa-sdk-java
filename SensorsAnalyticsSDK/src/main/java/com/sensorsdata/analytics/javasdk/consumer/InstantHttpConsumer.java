package com.sensorsdata.analytics.javasdk.consumer;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;

public class InstantHttpConsumer extends HttpConsumer {

    public InstantHttpConsumer(String serverUrl, int timeoutSec) {
        super(serverUrl, timeoutSec);
    }

    public InstantHttpConsumer(String serverUrl, Map<String, String> httpHeaders) {
        super(serverUrl, httpHeaders);
    }

    InstantHttpConsumer(String serverUrl, Map<String, String> httpHeaders, int timeoutSec) {
        super(serverUrl, httpHeaders, timeoutSec);
    }

    public InstantHttpConsumer(
            HttpClientBuilder httpClientBuilder,
            String serverUrl,
            Map<String, String> httpHeaders) {
        super(httpClientBuilder, serverUrl, httpHeaders);
    }

    public InstantHttpConsumer(
            HttpClientBuilder httpClientBuilder, String serverUrl, int timeoutSec) {
        super(httpClientBuilder, serverUrl, timeoutSec);
    }

    InstantHttpConsumer(
            HttpClientBuilder httpClientBuilder,
            String serverUrl,
            Map<String, String> httpHeaders,
            int timeoutSec) {
        super(httpClientBuilder, serverUrl, httpHeaders, timeoutSec);
    }

    @Override
    UrlEncodedFormEntity getHttpEntry(final String data) throws IOException {
        List<NameValuePair> nameValuePairs = getNameValuePairs(data);
        nameValuePairs.add(new BasicNameValuePair("instant_event", "true"));
        return new UrlEncodedFormEntity(nameValuePairs);
    }
}
