package com.sensorsdata.analytics.javasdk.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sensorsdata.analytics.javasdk.util.SensorsAnalyticsUtil;

import java.io.FileNotFoundException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

class InnerLoggingConsumer implements Consumer {

    // 默认缓存限制为 1G
    private static final int BUFFER_LIMITATION = 1024 * 1024 * 1024;
    private final ObjectMapper jsonMapper;
    private final String filenamePrefix;
    private final StringBuilder messageBuffer;
    private final int bufferSize;
    private final SimpleDateFormat simpleDateFormat;

    private final LoggingFileWriterFactory fileWriterFactory;
    private LoggingFileWriter fileWriter;

    InnerLoggingConsumer(
        LoggingFileWriterFactory fileWriterFactory,
        String filenamePrefix,
        int bufferSize, LogSplitMode splitMode) {
        this.fileWriterFactory = fileWriterFactory;
        this.filenamePrefix = filenamePrefix;
        this.jsonMapper = SensorsAnalyticsUtil.getJsonObjectMapper();
        this.messageBuffer = new StringBuilder(bufferSize);
        this.bufferSize = bufferSize;
        if (splitMode == LogSplitMode.HOUR) {
            this.simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
        } else {
            this.simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
        }
    }

    @Override
    public synchronized void send(Map<String, Object> message) {
        if (messageBuffer.length() < BUFFER_LIMITATION) {
            try {
                messageBuffer.append(jsonMapper.writeValueAsString(message));
                messageBuffer.append("\n");
            } catch (JsonProcessingException e) {
                throw new RuntimeException("fail to process json", e);
            }
        } else {
            throw new RuntimeException("logging buffer exceeded the allowed limitation.");
        }

        if (messageBuffer.length() >= bufferSize) {
            flush();
        }
    }

    private String constructFileName(Date now) {
        return filenamePrefix + "." + simpleDateFormat.format(now);
    }

    @Override
    public synchronized void flush() {
        if (messageBuffer.length() == 0) {
            return;
        }

        String filename = constructFileName(new Date());

        if (fileWriter != null && !fileWriter.isValid(filename)) {
            this.fileWriterFactory.closeFileWriter(fileWriter);
            fileWriter = null;
        }

        if (fileWriter == null) {
            try {
                fileWriter = this.fileWriterFactory.getFileWriter(filenamePrefix, filename);
            } catch (FileNotFoundException e) {
                throw new RuntimeException(e);
            }
        }

        if (fileWriter.write(messageBuffer)) {
            messageBuffer.setLength(0);
        }
    }

    @Override
    public synchronized void close() {
        flush();
        if (fileWriter != null) {
            this.fileWriterFactory.closeFileWriter(fileWriter);
            fileWriter = null;
        }
    }

    public enum LogSplitMode {
        DAY, HOUR
    }
}

interface LoggingFileWriter {
    boolean isValid(final String fileName);

    boolean write(final StringBuilder sb);

    void close();
}

interface LoggingFileWriterFactory {

    LoggingFileWriter getFileWriter(final String fileName, final String scheduleFileName)
            throws FileNotFoundException;

    void closeFileWriter(LoggingFileWriter writer);

}