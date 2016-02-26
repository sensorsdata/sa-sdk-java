package com.sensorsdata.analytics.javasdk.exceptions;

/**
 * 网络故障，无法连接Sensors Analytics服务器
 */
public class ConnectErrorException extends Exception {

  public ConnectErrorException(String message) {
    super(message);
  }

  public ConnectErrorException(Throwable error) {
    super(error);
  }

}
