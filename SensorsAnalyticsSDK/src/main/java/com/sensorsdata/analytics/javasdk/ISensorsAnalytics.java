package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMEventRecord;
import com.sensorsdata.analytics.javasdk.bean.IDMUserRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SensorsAnalyticsIdentity;
import com.sensorsdata.analytics.javasdk.bean.SuperPropertiesRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.bean.schema.DetailSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.IdentitySchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.ItemSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserEventSchema;
import com.sensorsdata.analytics.javasdk.bean.schema.UserSchema;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

import lombok.NonNull;

import java.util.Map;

/**
 * 外部可调用的接口方法
 *
 * @author fz
 * @version 1.0.0
 * @since 2021/05/25 11:58
 */
public interface ISensorsAnalytics {

    /**
     * 开启导入历史数据模式
     *
     * @param enableTimeFree true:表示开启；false:表示关闭；默认关闭
     */
    void setEnableTimeFree(@NonNull boolean enableTimeFree);

    /**
     * 设置公共属性
     *
     * @param propertiesRecord 公共属性实体
     */
    void registerSuperProperties(@NonNull SuperPropertiesRecord propertiesRecord);

    /**
     * 设置每个事件都带有的一些公共属性
     * <p>
     * 当track的Properties，superProperties和SDK自动生成的automaticProperties有相同的key时，遵循如下的优先级：
     * track.properties 高于 superProperties 高于 automaticProperties
     * <p>
     * 另外，当这个接口被多次调用时，是用新传入的数据去merge先前的数据
     * <p>
     * 例如，在调用接口前，dict是 {"a":1, "b": "bbb"}，传入的dict是 {"b": 123, "c": "asd"}，则merge后
     * 的结果是 {"a":1, "b": 123, "c": "asd"}
     *
     * @param superPropertiesMap 一个或多个公共属性
     */
    void registerSuperProperties(@NonNull Map<String, Object> superPropertiesMap);

    /**
     * 清除公共属性
     */
    void clearSuperProperties();

    /**
     * 全局接口；是否开启 $lib_detail 收集调用者信息，默认开启；在完成初始化之后，调用一次进行关闭
     *
     * @param enableCollectLibDetail -true:开启收集调用者信息；false 关闭收集信息，置为常量
     */
    void setGlobalEnableCollectMethodStack(boolean enableCollectLibDetail);

    /**
     * 记录事件
     *
     * @param eventRecord 事件消息对象
     *                    通过 {@link EventRecord.Builder} 来构造；
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void track(@NonNull EventRecord eventRecord) throws InvalidArgumentException;

    /**
     * 记录一个没有任何属性的事件
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param eventName  事件名称
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void track(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String eventName)
        throws InvalidArgumentException;

    /**
     * 记录一个拥有一个或多个属性的事件。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和
     * {@link java.util.List}；
     * 若属性包含 $time 字段，则它会覆盖事件的默认时间属性，该字段只接受{@link java.util.Date}类型；
     * 若属性包含 $project 字段，则它会指定事件导入的项目；
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param eventName  事件名称
     * @param properties 事件的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void track(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String eventName,
        Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 记录用户注册事件
     * <p>
     * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
     * http://www.sensorsdata.cn/manual/track_signup.html
     * 并在必要时联系我们的技术支持人员。
     *
     * @param loginId     登录 ID
     * @param anonymousId 匿名 ID
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void trackSignUp(@NonNull String loginId, @NonNull String anonymousId) throws InvalidArgumentException;

    /**
     * 记录用户注册事件
     * <p>
     * 这个接口是一个较为复杂的功能，请在使用前先阅读相关说明:
     * http://www.sensorsdata.cn/manual/track_signup.html
     * 并在必要时联系我们的技术支持人员。
     * <p>
     * 属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * 若属性包含 $time 字段，它会覆盖事件的默认时间属性，该字段只接受{@link java.util.Date}类型；
     * 若属性包含 $project 字段，则它会指定事件导入的项目；
     *
     * @param loginId     登录 ID
     * @param anonymousId 匿名 ID
     * @param properties  事件的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void trackSignUp(@NonNull String loginId, @NonNull String anonymousId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileSet(@NonNull UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileSet(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 设置用户的属性。这个接口只能设置单个key对应的内容，同样，如果已经存在，则覆盖，否则，新创建
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param property   属性名称
     * @param value      属性的值
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileSet(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException;

    /**
     * 首次设置用户的属性。
     * 属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 与profileSet接口不同的是：
     * 如果要设置的properties的key，在这个用户的profile中已经存在，则不处理，否则，新创建
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileSetOnce(@NonNull UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 首次设置用户的属性。
     * 属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 与profileSet接口不同的是：
     * 如果要设置的properties的key，在这个用户的profile中已经存在，则不处理，否则，新创建
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileSetOnce(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 首次设置用户的属性。这个接口只能设置单个key对应的内容。
     * 与profileSet接口不同的是，如果key的内容之前已经存在，则不处理，否则，重新创建
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param property   属性名称
     * @param value      属性的值
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileSetOnce(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException;

    /**
     * 为用户的数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileIncrement(@NonNull UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受
     * {@link Number}类型
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileIncrement(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 为用户的数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param property   属性名称
     * @param value      属性的值
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileIncrement(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull long value) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型
     * 必须为 {@link String}
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileAppend(@NonNull UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型
     * 必须为 {@link String}
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileAppend(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 为用户的数组类型的属性追加一个字符串
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param property   属性名称
     * @param value      属性的值
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileAppend(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property,
        @NonNull String value) throws InvalidArgumentException;

    /**
     * 删除用户已存在的一条或者多条属性
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileUnset(@NonNull UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 删除用户某一个属性
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param property   属性名称
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileUnset(@NonNull String distinctId, @NonNull boolean isLoginId, @NonNull String property)
        throws InvalidArgumentException;

    /**
     * 删除用户属性
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户属性名称列表，要删除的属性值请设置为 Boolean 类型的 true，如果要删除指定项目的用户属性，需正确传 $project 字段
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileUnset(@NonNull String distinctId, @NonNull boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 删除用户所有属性
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileDelete(@NonNull UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 删除用户所有属性
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @throws InvalidArgumentException distinctId 不符合命名规范时抛出该异常
     */
    void profileDelete(@NonNull String distinctId, @NonNull boolean isLoginId) throws InvalidArgumentException;

    /**
     * 增加item 记录
     *
     * @param itemRecord 维度表属性实体
     * @throws InvalidArgumentException itemId或itemType字段不合法则抛出该异常
     */
    void itemSet(@NonNull ItemRecord itemRecord) throws InvalidArgumentException;

    /**
     * 设置 item
     *
     * @param itemType   item 类型
     * @param itemId     item ID
     * @param properties item 相关属性
     * @throws InvalidArgumentException 取值不符合规范抛出该异常
     */
    void itemSet(@NonNull String itemType, @NonNull String itemId, @NonNull Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 删除维度表记录
     *
     * @param itemRecord 维度表属性实体
     * @throws InvalidArgumentException itemId或itemType字段不合法则抛出该异常
     */
    void itemDelete(@NonNull ItemRecord itemRecord) throws InvalidArgumentException;

    /**
     * 删除 item
     *
     * @param itemType   item 类型
     * @param itemId     item ID
     * @param properties item 相关属性
     * @throws InvalidArgumentException 取值不符合规范抛出该异常
     */
    void itemDelete(@NonNull String itemType, @NonNull String itemId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 用户标识绑定
     *
     * @param analyticsIdentity 用户标识 ID
     * @throws InvalidArgumentException 不合法参数异常
     */
    void bind(@NonNull SensorsAnalyticsIdentity... analyticsIdentity) throws InvalidArgumentException;

    /**
     * 用户标识解绑
     *
     * @param analyticsIdentity 用户标识 ID
     * @throws InvalidArgumentException 不合法参数异常
     * @see #unbind(String, String)
     */
    @Deprecated
    void unbind(@NonNull SensorsAnalyticsIdentity analyticsIdentity) throws InvalidArgumentException;

    /**
     * 用户标识解绑
     *
     * @param key   用户标识 key
     * @param value 用户标识值
     * @throws InvalidArgumentException 不合法参数异常
     */
    void unbind(@NonNull String key, @NonNull String value) throws InvalidArgumentException;

    /**
     * 使用用户标识 3.0 系统埋点事件
     *
     * @param analyticsIdentity 用户标识 ID
     * @param eventName         事件名
     * @param properties        事件属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void trackById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String eventName,
        Map<String, Object> properties) throws InvalidArgumentException;

    /**
     * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 如果要设置的 properties 的 key，之前在这个用户的 profile 中已经存在，则覆盖，否则，新创建
     *
     * @param analyticsIdentity 用户标识 ID
     * @param properties        用户属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileSetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
     *
     * @param analyticsIdentity 用户标识 ID
     * @param property          属性名
     * @param value             用于属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileSetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException;

    /**
     * 首次设置用户的属性。
     * 属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 与 profileSetById 接口不同的是：
     * 如果要设置的 properties 的 key，在这个用户的 profile 中已经存在，则不处理，否则，新创建
     *
     * @param analyticsIdentity 用户标识 ID
     * @param properties        用户属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileSetOnceById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 首次设置用户的属性。
     * 属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 与 profileSetById 接口不同的是：
     * 如果要设置的 properties 的 key，在这个用户的 profile 中已经存在，则不处理，否则，新创建
     *
     * @param analyticsIdentity 用户标识 ID
     * @param property          属性名
     * @param value             属性值
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileSetOnceById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull Object value) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为 0。属性取值只接受{@link Number}类型
     *
     * @param analyticsIdentity 用户标识 ID
     * @param properties        用户属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileIncrementById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为 0。属性取值只接受{@link Number}类型
     *
     * @param analyticsIdentity 用户标识 ID
     * @param property          属性名
     * @param value             属性值
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileIncrementById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, String property, long value)
        throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型必须为 {@link String}
     *
     * @param analyticsIdentity 用户标识 ID
     * @param properties        用户属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileAppendById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型必须为 {@link String}
     *
     * @param analyticsIdentity 用户标识 ID
     * @param property          属性名
     * @param value             属性值
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileAppendById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property,
        @NonNull String value) throws InvalidArgumentException;

    /**
     * 删除用户某一个属性
     *
     * @param analyticsIdentity 用户标识 ID
     * @param properties        用户属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileUnsetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 删除用户某一个属性
     *
     * @param analyticsIdentity 用户标识 ID
     * @param property          用户属性
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileUnsetById(@NonNull SensorsAnalyticsIdentity analyticsIdentity, @NonNull String property)
        throws InvalidArgumentException;

    /**
     * 删除用户所有属性
     *
     * @param analyticsIdentity 用户标识 ID
     * @throws InvalidArgumentException 不合法参数异常
     * @see #profileDeleteById(String, String)
     */
    @Deprecated
    void profileDeleteById(@NonNull SensorsAnalyticsIdentity analyticsIdentity) throws InvalidArgumentException;

    /**
     * 删除指定用户的所有属性
     *
     * @param key   用户标识 key
     * @param value 用户标识值
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileDeleteById(@NonNull String key, @NonNull String value) throws InvalidArgumentException;

    /**
     * 使用 IDM3.0 触发埋点事件
     *
     * @param idmEventRecord IDM 事件属性对象{@link com.sensorsdata.analytics.javasdk.bean.IDMEventRecord}
     * @throws InvalidArgumentException 参数不合法异常
     */
    void trackById(@NonNull IDMEventRecord idmEventRecord) throws InvalidArgumentException;

    /**
     * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 如果要设置的 properties 的 key，之前在这个用户的 profile 中已经存在，则覆盖，否则，新创建
     *
     * @param idmUserRecord 用户属性集合对象 {@link com.sensorsdata.analytics.javasdk.bean.IDMUserRecord}
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileSetById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException;

    /**
     * 首次设置用户的属性。
     * 属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     *
     * @param idmUserRecord 用户属性集合对象 {@link com.sensorsdata.analytics.javasdk.bean.IDMUserRecord}
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileSetOnceById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为 0。属性取值只接受{@link Number}类型
     *
     * @param idmUserRecord 用户属性集合对象 {@link com.sensorsdata.analytics.javasdk.bean.IDMUserRecord}
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileIncrementById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型必须为 {@link String}
     *
     * @param idmUserRecord 用户属性集合对象 {@link com.sensorsdata.analytics.javasdk.bean.IDMUserRecord}
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileAppendById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException;

    /**
     * 删除用户指定属性
     *
     * @param idmUserRecord 用户属性集合对象 {@link com.sensorsdata.analytics.javasdk.bean.IDMUserRecord}
     * @throws InvalidArgumentException 不合法参数异常
     */
    void profileUnsetById(@NonNull IDMUserRecord idmUserRecord) throws InvalidArgumentException;


    void track(@NonNull UserEventSchema userEventSchema) throws InvalidArgumentException;

    void track(@NonNull ItemEventSchema itemEventSchema) throws InvalidArgumentException;

    void bind(@NonNull IdentitySchema identitySchema) throws InvalidArgumentException;

    void unbind(@NonNull IdentitySchema identitySchema) throws InvalidArgumentException;

    void profileSet(@NonNull UserSchema userSchema) throws InvalidArgumentException;

    void profileSetOnce(@NonNull UserSchema userSchema) throws InvalidArgumentException;

    void profileIncrement(@NonNull UserSchema userSchema) throws InvalidArgumentException;

    void profileAppend(@NonNull UserSchema userSchema) throws InvalidArgumentException;

    void profileUnset(@NonNull UserSchema userSchema) throws InvalidArgumentException;

    void profileDelete(@NonNull String key, @NonNull String value) throws InvalidArgumentException;

    void profileDelete(@NonNull Long userId) throws InvalidArgumentException;

    void itemSet(@NonNull ItemSchema itemSchema) throws InvalidArgumentException;

    void itemDelete(@NonNull ItemSchema itemSchema) throws InvalidArgumentException;

    /**
     * 添加一条明细数据
     *
     * @param detailSchema 明细数据参数
     * @throws InvalidArgumentException 参数不合法异常
     */
    void detailSet(@NonNull DetailSchema detailSchema) throws InvalidArgumentException;

    /**
     * 删除一条明细数据
     *
     * @param detailSchema -明细数据参数
     * @throws InvalidArgumentException - 参数不合法异常
     */
    void detailDelete(@NonNull DetailSchema detailSchema) throws InvalidArgumentException;

    /**
     * 立即发送缓存中的所有日志
     */
    void flush();

    /**
     * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
     */
    void shutdown();
}
