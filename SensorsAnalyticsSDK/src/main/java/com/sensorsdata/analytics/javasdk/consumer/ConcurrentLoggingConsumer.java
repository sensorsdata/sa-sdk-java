package com.sensorsdata.analytics.javasdk.consumer;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.HashMap;
import java.util.Map;

public class ConcurrentLoggingConsumer extends InnerLoggingConsumer {

    public ConcurrentLoggingConsumer(final String filenamePrefix) throws IOException {
        this(filenamePrefix, null);
    }

    public ConcurrentLoggingConsumer(final String filenamePrefix, int bufferSize) throws IOException {
        this(filenamePrefix, null, bufferSize);
    }

    public ConcurrentLoggingConsumer(final String filenamePrefix, final String lockFileName) throws IOException {
        this(filenamePrefix, lockFileName, 8192);
    }

    public ConcurrentLoggingConsumer(
            String filenamePrefix,
            String lockFileName,
            int bufferSize) throws IOException {
        this(filenamePrefix, lockFileName, bufferSize, LogSplitMode.DAY);
    }

    public ConcurrentLoggingConsumer(
            String filenamePrefix,
            String lockFileName,
            int bufferSize,
            LogSplitMode splitMode) throws IOException {
        super(new InnerLoggingFileWriterFactory(lockFileName), filenamePrefix, bufferSize, splitMode);
    }

    static class InnerLoggingFileWriterFactory implements LoggingFileWriterFactory {

        private String lockFileName;

        InnerLoggingFileWriterFactory(String lockFileName) {
            this.lockFileName = lockFileName;
        }

        @Override
        public LoggingFileWriter getFileWriter(String fileName, String scheduleFileName)
                throws FileNotFoundException {
            return InnerLoggingFileWriter.getInstance(scheduleFileName, lockFileName);
        }

        @Override
        public void closeFileWriter(LoggingFileWriter writer) {
            ConcurrentLoggingConsumer.InnerLoggingFileWriter
                    .removeInstance((ConcurrentLoggingConsumer.InnerLoggingFileWriter) writer);
        }
    }

    static class InnerLoggingFileWriter implements LoggingFileWriter {
        private final Object fileLock = new Object();
        private final String fileName;
        private final String lockFileName;
        private FileOutputStream outputStream;
        private FileOutputStream lockStream;
        private int refCount;

        private static final Map<String, InnerLoggingFileWriter> instances;

        static {
            instances = new HashMap<String, InnerLoggingFileWriter>();
        }

        static InnerLoggingFileWriter getInstance(final String fileName, final String lockFileName) throws FileNotFoundException {
            synchronized (instances) {
                if (!instances.containsKey(fileName)) {
                    instances.put(fileName, new InnerLoggingFileWriter(fileName, lockFileName));
                }

                InnerLoggingFileWriter writer = instances.get(fileName);
                writer.refCount = writer.refCount + 1;
                return writer;
            }
        }

        static void removeInstance(final InnerLoggingFileWriter writer) {
            synchronized (instances) {
                writer.refCount = writer.refCount - 1;
                if (writer.refCount == 0) {
                    writer.close();
                    instances.remove(writer.fileName);
                }
            }
        }

        private InnerLoggingFileWriter(final String fileName, final String lockFileName) throws FileNotFoundException {
            this.fileName = fileName;
            this.lockFileName = lockFileName;
            this.refCount = 0;
            initLock();
        }

        public void close() {
            try {
                outputStream.close();
            } catch (Exception e) {
                throw new RuntimeException("fail to close output stream.", e);
            }
        }

        public boolean isValid(final String fileName) {
            return this.fileName.equals(fileName);
        }

        public boolean write(final StringBuilder sb) {
            synchronized (fileLock) {
                FileLock lock = null;
                try {
                    final FileChannel channel = lockStream.getChannel();
                    if (!channel.isOpen()) {
                        lockStream.close();
                        outputStream.close();
                        initLock();
                    }
                    lock = channel.lock(0, Long.MAX_VALUE, false);
                    outputStream.write(sb.toString().getBytes("UTF-8"));
                } catch (Exception e) {
                    throw new RuntimeException("fail to write file.", e);
                } finally {
                    if (lock != null) {
                        try {
                            lock.release();
                        } catch (IOException e) {
                            throw new RuntimeException("fail to release file lock.", e);
                        }
                    }
                }
            }

            return true;
        }

        private void initLock() throws FileNotFoundException {
            this.outputStream = new FileOutputStream(fileName, true);
            if (lockFileName != null) {
                this.lockStream = new FileOutputStream(lockFileName, true);
            } else {
                this.lockStream = this.outputStream;
            }
        }
    }
}