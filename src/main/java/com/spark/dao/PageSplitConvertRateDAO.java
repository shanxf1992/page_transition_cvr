package com.spark.dao;

import com.spark.pojo.PageSplitConvertRate;
import com.spark.util.JDBCUtils;

public class PageSplitConvertRateDAO {

    public void insert(PageSplitConvertRate pageSplitConvertRate) {

        String sql = "insert into page_split_convert_rate values(?, ?)";
        Object[] params = new Object[]{pageSplitConvertRate.getTaskId(), pageSplitConvertRate.getConvertRate()};

        JDBCUtils instance = JDBCUtils.getInstance();
        instance.executeUpdate(sql, params);
    }

}
