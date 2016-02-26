package com.sensorsdata.analytics.javasdk.exceptions;

/**
 * SensorsAnalytics返回异常，flush失败
 */
public class FlushErrorException extends Exception {

  public FlushErrorException(String message) {
    super(message);
  }

  public FlushErrorException(Throwable error) {
    super(error);
  }

}
