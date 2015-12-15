package com.sensorsdata.analytics.java.sdk.exceptions;

/**
 * 超出缓存队列最大长度限制
 */
public class QueueLimitExceededException extends Exception {

  public QueueLimitExceededException(String message) {
    super(message);
  }

  public QueueLimitExceededException(Throwable error) {
    super(error);
  }

}
