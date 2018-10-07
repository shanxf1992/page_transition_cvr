package com.spark.accumulator;

import org.apache.spark.util.AccumulatorV2;

/**
 * 自定义累加器, 计算起始页面的PV
 */
public class StartPageAccumulator extends AccumulatorV2<Long, Long> {

    //初始化页面PV 为 0L
    Long value = 0L;

    public boolean isZero() {
        return value == 0;
    }

    public AccumulatorV2<Long, Long> copy() {
        StartPageAccumulator accumulatorV2 = new StartPageAccumulator();
        accumulatorV2.value = this.value;
        return accumulatorV2;
    }

    public void reset() {
        this.value = 0L;
    }

    public void add(Long v) {
        this.value = this.value + v;
    }

    public void merge(AccumulatorV2<Long, Long> other) {
        this.value += other.value();
    }

    public Long value() {
        return this.value;
    }
}
