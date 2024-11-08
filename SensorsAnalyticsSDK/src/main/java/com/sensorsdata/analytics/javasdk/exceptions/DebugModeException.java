package com.sensorsdata.analytics.javasdk.exceptions;

/** Debug模式下的错误 */
public class DebugModeException extends RuntimeException {

    public DebugModeException(String message) {
        super(message);
    }

    public DebugModeException(Throwable error) {
        super(error);
    }

    public DebugModeException(String message, Throwable error) {
        super(message, error);
    }
}
