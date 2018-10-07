package com.spark.page;

import com.alibaba.fastjson.JSONObject;
import com.spark.accumulator.StartPageAccumulator;
import com.spark.constant.Constants;
import com.spark.dao.PageSplitConvertRateDAO;
import com.spark.dao.TaskDAO;
import com.spark.daofactory.DAOFactory;
import com.spark.mockdata.MockData;
import com.spark.pojo.PageSplitConvertRate;
import com.spark.pojo.Task;
import com.spark.util.DateUtils;
import com.spark.util.NumberUtils;
import com.spark.util.ParamUtils;
import com.spark.util.SparkUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.util.AccumulatorV2;
import scala.Tuple2;

import java.util.*;

/**
 * 页面单跳转化率作业
 */
public class PageOneStepCVRSpark {
    public static void main(String[] args) {

        //1. 创建sparkContext 上下文
        SparkSession spark = SparkSession
                .builder()
                .master(Constants.SPARK_LOCAL)
                .appName(Constants.SPARK_APP_NAME_PAGE)
                .enableHiveSupport()
                .getOrCreate();
        //2 生成模拟数据
        MockData.mock(spark);
        //3 查询任务获取任务的参数
        TaskDAO taskDAO = DAOFactory.getTaskDAO();
        //查询出对应的需要执行的任务
        long taskId = ParamUtils.getTaskIdFromArgs(args);
        Task task = taskDAO.findById(taskId);
        // 解析用户任务的指定参数
        JSONObject taskParam = JSONObject.parseObject(task.getTaskParam());

        //4 根据任务参数, 查询指定范围内的用户访问行为数据
        JavaRDD<Row> actionRDD = SparkUtil.getActionRDDByDateRange(spark, taskParam);

        //5 将用户访问数据映射成 <sessionId, row>的形式, 因为页面单跳是基于用户的session, 页面切片的生成是基于用户session粒度的
        // 映射完成后, 根据session粒度进行聚合. 然后基于session粒度的访问行为进行分析
        JavaPairRDD<String, Iterable<Row>> aggrSessinoRDD  = getAggrSessionRDD(actionRDD);

        //6 核心: 每个session的单跳切片页面的生成, 以及页面流匹配算法
        //8 计算页面流起始页面的 PV
        //TODO:通过自定义累加器的方式, 在计算 页面切片的时候计算起始页面的PV
        AccumulatorV2<Long, Long> startPageCount = new StartPageAccumulator();
        spark.sparkContext().register(startPageCount);

        JavaPairRDD<String, Long> pageSplitRDD = generateAndMatchPageSplit(spark, aggrSessinoRDD, taskParam, startPageCount);

        //7 获取访问session页面单跳次数 [(3_5, 3), (5_7, 4) ....]
        Map<String, Long> pageSplitPV = pageSplitRDD.countByKey();

        //8 计算页面流起始页面的 PV
        Long startPagePV = startPageCount.value();

        //9 计算各个页面之间的转化率
        Map<String, Double> pageSplitCVR = getPageSplitCVR(startPagePV, pageSplitPV, taskParam);

        //10 持久化页面转化率
        persistPageCVR(taskId, pageSplitCVR);

    }


    /**
     * 将用户访问行为数据进行session粒度的聚合
     * @param actionRDD 用户访问行为数据
     * @return
     */
    private static JavaPairRDD<String, Iterable<Row>> getAggrSessionRDD(JavaRDD<Row> actionRDD) {
        return actionRDD.mapToPair(new PairFunction<Row, String, Row>() {
            public Tuple2<String, Row> call(Row row) throws Exception {
                return new Tuple2<String, Row>(row.getString(2), row);
            }
        }).groupByKey();
    }

    /**
     * 页面切片的生成与匹配
     * @param spark
     * @param aggrSessinoRDD 按session粒度聚合的访问信息
     * @param param
     * @return
     */
    private static JavaPairRDD<String, Long> generateAndMatchPageSplit(SparkSession spark, JavaPairRDD<String, Iterable<Row>> aggrSessinoRDD, JSONObject param, AccumulatorV2<Long, Long> startPageCount) {

        //用户指定的页面流参数 ; 3,4,5,6,8...
        String pageFlow = ParamUtils.getParam(param, Constants.PARAM_TARGET_PAGE_FLOW);

        //将需要的页面流参数设置成广播变量
        //获取用户指定的页面流 ; [3, 4, 5, 6, 8...]
        String[] pages = pageFlow.split(",");

        //获取用户指定的页面切片
        Set<String> pageSplitSet = new HashSet<String>();
        for (int i = 1; i < pages.length; i++) {
            pageSplitSet.add(pages[i - 1] + "_" + pages[i]);
        }

        //将用户执行的页面流切片, 设置为广播变量
        JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
        Broadcast<Long> startPageIdBroadcast = sc.broadcast(Long.parseLong(pages[0]));
        Broadcast<Set<String>> pageSplitBroadcast= sc.broadcast(pageSplitSet);


        return aggrSessinoRDD.flatMapToPair(new PairFlatMapFunction<Tuple2<String, Iterable<Row>>, String, Long>() {
            public Iterator<Tuple2<String, Long>> call(Tuple2<String, Iterable<Row>> tuple2) throws Exception {

                List<Tuple2<String, Long>> list = new ArrayList<Tuple2<String, Long>>();

                //用户指定页面切片集合
                Set<String> pageSplitSet = pageSplitBroadcast.getValue();
                Long startPageId = startPageIdBroadcast.getValue();

                //迭代每个session对应的访问行为, 计算对应的页面切片
                //但是对于对于一个session的多个访问行为, 默认情况下是乱序的, 需要按照实现顺序排序
                //所以首先, 对session的访问行为按照时间顺序进行排序
                List<Row> rows = new ArrayList<Row>();
                for (Row row : tuple2._2) {
                    rows.add(row);
                }

                //对session的访问行为进行排序, 按照时间排序
                Collections.sort(rows, new Comparator<Row>() {
                    public int compare(Row row1, Row row2) {
                        Date date1 = DateUtils.parseTime(row1.getString(4));
                        Date date2 = DateUtils.parseTime(row2.getString(4));
                        return (int) (date1.getTime() - date2.getTime());
                    }
                });

                //页面切片的生成, 页面流的匹配
                Long lastPageId = null;
                for (Row row : rows) {
                    long pageId = row.getLong(3);

                    //判断是否是第起始页, 如果是则累加器增加
                    if (pageId == startPageId) startPageCount.add(1L);

                    if (lastPageId == null) {
                        lastPageId = pageId;
                        continue;
                    }
                    String targetPageSplit = lastPageId + "_" + pageId;
                    if (pageSplitSet.contains(targetPageSplit)) {
                        list.add(new Tuple2<String, Long>(targetPageSplit, 1L));
                    }
                    lastPageId = pageId;
                }

                return list.iterator();
            }
        });
    }

    /**
     * 计算各个页面切片之间的单跳转化率
     *
     * @param startPagePV 起始页面的PV
     * @param pageSplitPV 各个切片之间的PV
     * @return
     */
    private static Map<String, Double> getPageSplitCVR(Long startPagePV, Map<String, Long> pageSplitPV, JSONObject param) {

        //
        Map<String, Double> pageSplitCVRMap = new HashMap<>();

        //用户指定的页面流参数 ; 3,5,7,6,8...
        String[] pageFlow = ParamUtils.getParam(param, Constants.PARAM_TARGET_PAGE_FLOW).split(",");

        Long lastPageSplitPV = 0L;
        for (int i = 1; i < pageFlow.length; i++) {
            String targetPageSplit = pageFlow[i - 1] + "_" + pageFlow[i];

            //Map<String, Long> pageSplitPV: [(3_5, 3), (5_7, 4) ....]
            Long targetPageSplitPV = pageSplitPV.get(targetPageSplit);

            Double targetPageCVR = 0.0;
            // 如果计算第二个页面到第一个页面的转化率, 直接除以起始页面的个数
            if (i == 1) {
                targetPageCVR = targetPageSplitPV.doubleValue() / startPagePV.doubleValue();
                pageSplitCVRMap.put(targetPageSplit, NumberUtils.formatDouble(targetPageCVR, 2));
            } else {
                targetPageCVR = targetPageSplitPV.doubleValue() / lastPageSplitPV.doubleValue();
                pageSplitCVRMap.put(targetPageSplit, NumberUtils.formatDouble(targetPageCVR, 2));
            }

            lastPageSplitPV = targetPageSplitPV;
        }

        return pageSplitCVRMap;
    }

    /**
     * 将计算的页面转化率持久化到数据库
     * @param taskId
     * @param getPageSplitCVR
     */
    private static void persistPageCVR(Long taskId, Map<String, Double> getPageSplitCVR) {
        StringBuffer stringBuffer = new StringBuffer();
        for (Map.Entry<String, Double> entry : getPageSplitCVR.entrySet()) {
            String pageSplit = entry.getKey();
            double convertRate = entry.getValue();
            stringBuffer.append(pageSplit + "=" + convertRate + "|");
        }

        //拼接各个页面切片的转化率
        String pageCVR = stringBuffer.substring(0, stringBuffer.length() - 1).toString();

        //封装实体类
        PageSplitConvertRate pageSplitConvertRate = new PageSplitConvertRate();
        pageSplitConvertRate.setTaskId(taskId.intValue());
        pageSplitConvertRate.setConvertRate(pageCVR);

        //插入数据库
        PageSplitConvertRateDAO pageSplitConvertRateDAO = DAOFactory.getPageSplitConvertRateDAO();
        pageSplitConvertRateDAO.insert(pageSplitConvertRate);

    }
}
