package com.sensorsdata.analytics.javasdk;

import com.sensorsdata.analytics.javasdk.bean.EventRecord;
import com.sensorsdata.analytics.javasdk.bean.ItemRecord;
import com.sensorsdata.analytics.javasdk.bean.SuperPropertiesRecord;
import com.sensorsdata.analytics.javasdk.bean.UserRecord;
import com.sensorsdata.analytics.javasdk.exceptions.InvalidArgumentException;

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
    void setEnableTimeFree(boolean enableTimeFree);

    /**
     * 设置公共属性
     *
     * @param propertiesRecord 公共属性实体
     */
    void registerSuperProperties(SuperPropertiesRecord propertiesRecord);

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
    void registerSuperProperties(Map<String, Object> superPropertiesMap);

    /**
     * 清除公共属性
     */
    void clearSuperProperties();

    /**
     * 记录事件
     *
     * @param eventRecord 事件消息对象
     *                    通过 {@link EventRecord.Builder} 来构造；
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void track(EventRecord eventRecord) throws InvalidArgumentException;

    /**
     * 记录一个没有任何属性的事件
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param eventName  事件名称
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void track(String distinctId, boolean isLoginId, String eventName) throws InvalidArgumentException;

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
    void track(String distinctId, boolean isLoginId, String eventName, Map<String, Object> properties)
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
    void trackSignUp(String loginId, String anonymousId) throws InvalidArgumentException;

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
    void trackSignUp(String loginId, String anonymousId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 设置用户的属性。属性取值可接受类型为{@link Number}, {@link String}, {@link java.util.Date}和{@link java.util.List}；
     * <p>
     * 如果要设置的properties的key，之前在这个用户的profile中已经存在，则覆盖，否则，新创建
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileSet(UserRecord userRecord) throws InvalidArgumentException;

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
    void profileSet(String distinctId, boolean isLoginId, Map<String, Object> properties)
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
    void profileSet(String distinctId, boolean isLoginId, String property, Object value)
        throws InvalidArgumentException;

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
    void profileSetOnce(UserRecord userRecord) throws InvalidArgumentException;

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
    void profileSetOnce(String distinctId, boolean isLoginId, Map<String, Object> properties)
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
    void profileSetOnce(String distinctId, boolean isLoginId, String property, Object value)
        throws InvalidArgumentException;

    /**
     * 为用户的数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileIncrement(UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数值类型的属性累加一个数值，若该属性不存在，则创建它并设置默认值为0。属性取值只接受
     * {@link Number}类型
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileIncrement(String distinctId, boolean isLoginId, Map<String, Object> properties)
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
    void profileIncrement(String distinctId, boolean isLoginId, String property, long value)
        throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型
     * 必须为 {@link String}
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileAppend(UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 为用户的一个或多个数组类型的属性追加字符串，属性取值类型必须为 {@link java.util.List}，且列表中元素的类型
     * 必须为 {@link String}
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户的属性
     * @throws InvalidArgumentException eventName 或 properties 不符合命名规范和类型规范时抛出该异常
     */
    void profileAppend(String distinctId, boolean isLoginId, Map<String, Object> properties)
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
    void profileAppend(String distinctId, boolean isLoginId, String property, String value)
        throws InvalidArgumentException;

    /**
     * 删除用户已存在的一条或者多条属性
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileUnset(UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 删除用户某一个属性
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param property   属性名称
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileUnset(String distinctId, boolean isLoginId, String property) throws InvalidArgumentException;

    /**
     * 删除用户属性
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @param properties 用户属性名称列表，要删除的属性值请设置为 Boolean 类型的 true，如果要删除指定项目的用户属性，需正确传 $project 字段
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileUnset(String distinctId, boolean isLoginId, Map<String, Object> properties)
        throws InvalidArgumentException;

    /**
     * 删除用户所有属性
     *
     * @param userRecord 用户属性实体
     * @throws InvalidArgumentException 用户属性类型或者用户ID不合法则抛出该异常
     */
    void profileDelete(UserRecord userRecord) throws InvalidArgumentException;

    /**
     * 删除用户所有属性
     *
     * @param distinctId 用户 ID
     * @param isLoginId  用户 ID 是否是登录 ID，false 表示该 ID 是一个匿名 ID
     * @throws InvalidArgumentException distinctId 不符合命名规范时抛出该异常
     */
    void profileDelete(String distinctId, boolean isLoginId) throws InvalidArgumentException;

    /**
     * 增加item 记录
     *
     * @param itemRecord 维度表属性实体
     * @throws InvalidArgumentException itemId或itemType字段不合法则抛出该异常
     */
    void itemSet(ItemRecord itemRecord) throws InvalidArgumentException;

    /**
     * 设置 item
     *
     * @param itemType   item 类型
     * @param itemId     item ID
     * @param properties item 相关属性
     * @throws InvalidArgumentException 取值不符合规范抛出该异常
     */
    void itemSet(String itemType, String itemId, Map<String, Object> properties) throws InvalidArgumentException;

    /**
     * 删除维度表记录
     *
     * @param itemRecord 维度表属性实体
     * @throws InvalidArgumentException itemId或itemType字段不合法则抛出该异常
     */
    void itemDelete(ItemRecord itemRecord) throws InvalidArgumentException;

    /**
     * 删除 item
     *
     * @param itemType   item 类型
     * @param itemId     item ID
     * @param properties item 相关属性
     * @throws InvalidArgumentException 取值不符合规范抛出该异常
     */
    void itemDelete(String itemType, String itemId, Map<String, Object> properties) throws InvalidArgumentException;

    /**
     * 立即发送缓存中的所有日志
     */
    void flush();

    /**
     * 停止SensorsDataAPI所有线程，API停止前会清空所有本地数据
     */
    void shutdown();
}
