package com.spark.util;

import com.alibaba.fastjson.JSONObject;
import com.spark.conf.ConfigurationManager;
import com.spark.constant.Constants;
import com.spark.mockdata.MockData;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * spark 工具类
 */
public class SparkUtil {

    /**
     * 根据配置文件中的参数, 设置spark作业是否本地运行
     * @param sparkConf
     */
    public static void setMaster(SparkConf sparkConf) {
        String local = Constants.SPARK_LOCAL;
        if ("local".equals(local)) {
            sparkConf.setMaster("local");
        }
    }

    /**
     * 生成模拟数据
     * @param spark
     */
    public static void mockData(SparkSession spark) {
        //判断是否是本地模式
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());

        Boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(spark);
        }
    }

    /**
     * 从 user_visit_action 表中筛选出指定日期范围内的记录
     * @return
     */
    public static JavaRDD<Row> getActionRDDByDateRange(SparkSession spark, JSONObject param) {
        String startDate = ParamUtils.getParam(param, Constants.PARAM_START_DATE);
        String endDate = ParamUtils.getParam(param, Constants.PARAM_END_DATE);

        String sql = "select * from user_visit_action " +
                "where date >= '" + startDate + "' and date <= '" + endDate + "'";

        Dataset<Row> dataset = spark.sql(sql);


        //return dataset.toJavaRDD().repartition(100);
        return dataset.toJavaRDD();
    }


}
