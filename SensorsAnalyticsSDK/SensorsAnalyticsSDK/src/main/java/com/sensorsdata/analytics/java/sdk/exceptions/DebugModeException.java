package com.sensorsdata.analytics.java.sdk.exceptions;

/**
 * Debug模式下的错误
 */
public class DebugModeException extends RuntimeException {

  public DebugModeException(String message) {
    super(message);
  }

  public DebugModeException(Throwable error) {
    super(error);
  }

}
