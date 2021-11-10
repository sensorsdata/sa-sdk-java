package com.sensorsdata.analytics.javasdk.consumer;

import com.sensorsdata.analytics.javasdk.bean.FailedData;

/**
 * 异常数据回调
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2021/11/05 23:47
 */
public interface Callback {
  /**
   * 返回发送失败的数据
   *
   * @param failedData 失败数据
   */
  void onFailed(FailedData failedData);

}
