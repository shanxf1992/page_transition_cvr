package com.spark.constant;

public interface Constants {
    /**
     * 项目配置相关的常量
     */
    String JDBC_DRIVER = "jdbc.driverClassName";
    String JDBC_URL = "jdbc.url";
    String JDBC_USER = "jdbc.username";
    String JDBC_PASSWORD = "jdbc.password";
    String JDBC_DATASOURCE_SIZE = "jdbc.size";

    /**
     * spark 作业相关的常量
     */
    String SPARK_LOCAL = "local";
    String SPARK_APP_NAME_PAGE = "PageOneStepCVRSpark";;



    //任务相关的常量
    String PARAM_START_DATE = "startDate";
    String PARAM_END_DATE = "endDate";;
    String PARAM_TARGET_PAGE_FLOW = "targetPageFlow";



}
