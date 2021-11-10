package com.sensorsdata.analytics.javasdk.bean;



import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.List;
import java.util.Map;

/**
 * 发送异常数据包装类
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/06 10:35
 */
@Setter
@Getter
@AllArgsConstructor
@ToString
public class FailedData {
  /**
   * 失败原因
   */
  private String failedMessage;
  /**
   * 失败数据
   */
  private List<Map<String,Object>> failedData;
}
