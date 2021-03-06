package com.sensorsdata.analytics.javasdk;

/**
 * java SDK 全局变量
 *
 * @author fz
 * @version 1.0.0
 * @since 2021/05/25 10:29
 */
public class SensorsConst {

    private SensorsConst() {
    }

    /**
     * 当前JDK版本号，注意要和pom文件里面的version保持一致
     */
    public static final String SDK_VERSION = "3.2.0";
    /**
     * 当前语言类型
     */
    public static final String LIB = "Java";
    /**
     * 事件上报类型
     */
    public static final String TRACK_ACTION_TYPE = "track";
    public static final String TRACK_SIGN_UP_ACTION_TYPE = "track_signup";
    public static final String PROFILE_SET_ACTION_TYPE = "profile_set";
    public static final String PROFILE_SET_ONCE_ACTION_TYPE = "profile_set_once";
    public static final String PROFILE_APPEND_ACTION_TYPE = "profile_append";
    public static final String PROFILE_INCREMENT_ACTION_TYPE = "profile_increment";
    public static final String PROFILE_UNSET_ACTION_TYPE = "profile_unset";
    public static final String PROFILE_DELETE_ACTION_TYPE = "profile_delete";
    public static final String ITEM_SET_ACTION_TYPE = "item_set";
    public static final String ITEM_DELETE_ACTION_TYPE = "item_delete";
    /**
     * 系统预置属性
     */
    public static final String PROJECT_SYSTEM_ATTR = "$project";
    public static final String TINE_SYSTEM_ATTR = "$time";
    public static final String TOKEN_SYSTEM_ATTR = "$token";
    static final String LOGIN_SYSTEM_ATTR = "$is_login_id";
    static final String APP_VERSION_SYSTEM_ATTR = "$app_version";
    static final String LIB_SYSTEM_ATTR = "$lib";
    static final String LIB_VERSION_SYSTEM_ATTR = "$lib_version";
    static final String LIB_METHOD_SYSTEM_ATTR = "$lib_method";
    static final String LIB_DETAIL_SYSTEM_ATTR = "$lib_detail";
    static final String SIGN_UP_SYSTEM_ATTR = "$SignUp";


}
