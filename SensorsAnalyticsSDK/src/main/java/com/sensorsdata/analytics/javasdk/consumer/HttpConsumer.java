package com.sensorsdata.analytics.javasdk.consumer;

import com.sensorsdata.analytics.javasdk.SensorsConst;
import com.sensorsdata.analytics.javasdk.util.Base64Coder;

import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPOutputStream;

class HttpConsumer implements Closeable {
    CloseableHttpClient httpClient;
    final String serverUrl;
    final Map<String, String> httpHeaders;
    final boolean compressData;
    final RequestConfig requestConfig;

    public HttpConsumer(String serverUrl, int timeoutSec) {
        this(serverUrl, null, timeoutSec);
    }

    public HttpConsumer(String serverUrl, Map<String, String> httpHeaders) {
        this(serverUrl, httpHeaders, 3);
    }

    HttpConsumer(String serverUrl, Map<String, String> httpHeaders, int timeoutSec) {
        this.serverUrl = serverUrl.trim();
        this.httpHeaders = httpHeaders;
        this.compressData = true;
        int timeout = timeoutSec * 1000;
        this.requestConfig = RequestConfig.custom().setConnectionRequestTimeout(timeout)
            .setConnectTimeout(timeout).setSocketTimeout(timeout).build();
        this.httpClient = HttpClients.custom()
            .setUserAgent(String.format("SensorsAnalytics Java SDK %s", SensorsConst.SDK_VERSION))
            .setDefaultRequestConfig(requestConfig)
            .build();
    }

    void consume(final String data) throws IOException, HttpConsumerException {
        HttpUriRequest request = getHttpRequest(data);
        CloseableHttpResponse response = null;
        if (httpClient == null) {
            httpClient = HttpClients.custom()
                .setUserAgent(String.format("SensorsAnalytics Java SDK %s", SensorsConst.SDK_VERSION))
                .setDefaultRequestConfig(requestConfig)
                .build();
        }
        try {
            response = httpClient.execute(request);
            int httpStatusCode = response.getStatusLine().getStatusCode();
            if (httpStatusCode < 200 || httpStatusCode >= 300) {
                String httpContent = new String(EntityUtils.toByteArray(response.getEntity()), StandardCharsets.UTF_8);
                throw new HttpConsumerException(
                        String.format("Unexpected response %d from Sensors Analytics: %s", httpStatusCode, httpContent), data,
                        httpStatusCode, httpContent);
            }
        } finally {
            if (response != null) {
                response.close();
            }
        }
    }

    HttpUriRequest getHttpRequest(final String data) throws IOException {
        HttpPost httpPost = new HttpPost(this.serverUrl);
        httpPost.setEntity(getHttpEntry(data));

        if (this.httpHeaders != null) {
            for (Map.Entry<String, String> entry : this.httpHeaders.entrySet()) {
                httpPost.addHeader(entry.getKey(), entry.getValue());
            }
        }

        return httpPost;
    }

    UrlEncodedFormEntity getHttpEntry(final String data) throws IOException {
        byte[] bytes = data.getBytes(Charset.forName("UTF-8"));

        List<NameValuePair> nameValuePairs = new ArrayList<NameValuePair>();

        if (compressData) {
            ByteArrayOutputStream os = new ByteArrayOutputStream(bytes.length);
            GZIPOutputStream gos = new GZIPOutputStream(os);
            gos.write(bytes);
            gos.close();
            byte[] compressed = os.toByteArray();
            os.close();

            nameValuePairs.add(new BasicNameValuePair("gzip", "1"));
            nameValuePairs.add(new BasicNameValuePair("data_list", new String(Base64Coder.encode
                    (compressed))));
        } else {
            nameValuePairs.add(new BasicNameValuePair("gzip", "0"));
            nameValuePairs.add(new BasicNameValuePair("data_list", new String(Base64Coder.encode
                    (bytes))));
        }

        return new UrlEncodedFormEntity(nameValuePairs);
    }

    @Override
    public synchronized void close() {
        try {
            if (httpClient != null) {
                httpClient.close();
                httpClient = null;
            }
        } catch (IOException ignored) {
            // do nothing
        }
    }

    static class HttpConsumerException extends Exception {

        HttpConsumerException(String error, String sendingData, int httpStatusCode, String
                httpContent) {
            super(error);
            this.sendingData = sendingData;
            this.httpStatusCode = httpStatusCode;
            this.httpContent = httpContent;
        }

        String getSendingData() {
            return sendingData;
        }

        int getHttpStatusCode() {
            return httpStatusCode;
        }

        String getHttpContent() {
            return httpContent;
        }

        final String sendingData;
        final int httpStatusCode;
        final String httpContent;
    }
}
