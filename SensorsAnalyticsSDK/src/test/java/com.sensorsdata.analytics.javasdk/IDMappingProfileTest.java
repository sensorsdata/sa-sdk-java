package com.sensorsdata.analytics.javasdk;

import static org.junit.Assert.assertEquals;

import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import org.junit.Test;

import java.util.ArrayList;

/**
 * id-mapping profile 相关接口单元测试
 * 版本支持 3.3.0+
 *
 * @author fangzhuo
 * @version 1.0.0
 * @since 2022/03/12 11:10
 */
public class IDMappingProfileTest extends SensorsBaseTest {

  private final String loginId = "fz123";
  private final String email = "fz@163.com";
  //------------------------------3.3.0----------------------------

  /**
   * <p>
   * 用户使用 IDMUserRecord 调用 profileSetById 接口；
   * </p>
   * 期望生成的数据结构符合神策数据格式
   */
  @Test
  public void checkProfileSetById() throws InvalidArgumentException {
    final SensorsAnalyticsIdentity identity = SensorsAnalyticsIdentity.builder()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .build();
    sa.profileSetById(identity, "test", "ddd");
    assertIDM3UserData(data);
  }

  //------------------------------3.4.2----------------------------

  /**
   * <p>
   * 用户使用 IDMUserRecord 调用 profileSetById 接口；
   * </p>
   * 期望生成的数据结构符合神策数据格式
   */
  @Test
  public void checkProfileSetById01() throws InvalidArgumentException {
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .addProperty("hello", "dd")
        .build();
    sa.profileSetById(userRecord);
    assertIDM3UserData(data);
  }

  /**
   * <p>
   * 用户携带 $identity_login_id 使用 IDMUserRecord 调用 profileSetById 接口；
   * </p>
   * 期望生成的数据结构，distinct_id 取值为 $identity_login_id
   */
  @Test
  public void checkProfileSetByIdWithLoginId() throws InvalidArgumentException {
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .addProperty("hello", "dd")
        .build();
    sa.profileSetById(userRecord);
    assertEquals(loginId, data.get("distinct_id"));
  }

  /**
   * <p>
   * 用户携带 $identity_login_id，设置 distinct_id 使用 IDMUserRecord 调用 profileSetById 接口；
   * </p>
   * 期望生成的数据结构，distinct_id 取值为 $identity_login_id
   */
  @Test
  public void checkProfileSetByIdWithLoginId01() throws InvalidArgumentException {
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .setDistinctId("zzz")
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .addProperty("hello", "dd")
        .build();
    sa.profileSetById(userRecord);
    assertEquals("zzz", data.get("distinct_id"));
  }

  /**
   * <p>
   * 用户使用 IDMUserRecord 调用 profileSetOnceById 接口；
   * </p>
   * 期望生成的数据结构符合神策数据格式
   */
  @Test
  public void checkProfileSetOnceById() throws InvalidArgumentException {
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .addProperty("hello", "dd")
        .build();
    sa.profileSetOnceById(userRecord);
    assertIDM3UserData(data);
  }

  /**
   * <p>
   * 用户使用 IDMUserRecord 调用 profileIncrementById 接口；
   * </p>
   * 期望生成的数据结构符合神策数据格式
   */
  @Test
  public void checkProfileIncrementById() throws InvalidArgumentException {
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .addProperty("num", 33)
        .build();
    sa.profileIncrementById(userRecord);
    assertIDM3UserData(data);
  }

  /**
   * <p>
   * 用户使用 IDMUserRecord 调用 profileIncrementById 接口；
   * </p>
   * 期望生成的数据结构符合神策数据格式
   */
  @Test
  public void checkProfileAppendById() throws InvalidArgumentException {
    ArrayList<String> strings = new ArrayList<>();
    strings.add("movie");
    strings.add("swim");
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .addProperty("lists", strings)
        .build();
    sa.profileAppendById(userRecord);
    assertIDM3UserData(data);
  }

  /**
   * <p>
   * 用户使用 IDMUserRecord 调用 profileUnsetById 接口；
   * </p>
   * 期望生成的数据结构符合神策数据格式
   */
  @Test
  public void checkProfileUnsetById() throws InvalidArgumentException {
    IDMUserRecord userRecord = IDMUserRecord.starter()
        .addIdentityProperty(SensorsAnalyticsIdentity.LOGIN_ID, loginId)
        .addIdentityProperty(SensorsAnalyticsIdentity.EMAIL, email)
        .build();
    sa.profileUnsetById(userRecord);
    assertIDM3UserData(data);
  }
}
