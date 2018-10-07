package com.spark.daofactory;


import com.spark.dao.PageSplitConvertRateDAO;
import com.spark.dao.TaskDAO;

/**
 * DAO工厂方法
 */
public class DAOFactory {

    // 获取 TaskDAO 实例对象
    public static TaskDAO getTaskDAO() {
        return new TaskDAO();
    }

    public static PageSplitConvertRateDAO getPageSplitConvertRateDAO() {
        return new PageSplitConvertRateDAO();
    }
}
