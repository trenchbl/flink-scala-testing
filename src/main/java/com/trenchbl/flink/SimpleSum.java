package com.trenchbl.flink;

import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.co.CoProcessFunction;
import org.apache.flink.util.Collector;

public class SimpleSum extends CoProcessFunction<Integer, Integer, Integer> {

    private ValueState<Integer> sum;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);

        sum = getRuntimeContext().getState(new ValueStateDescriptor<Integer>("sum", Integer.class));
    }

    @Override
    public void processElement1(Integer integer, Context context, Collector<Integer> collector) throws Exception {
        if(sum.value() == null){
            sum.update(0 + integer);
        } else {
            sum.update(sum.value() + integer);
        }
        collector.collect(sum.value());
    }

    @Override
    public void processElement2(Integer integer, Context context, Collector<Integer> collector) throws Exception {
        if(sum.value() == null){
            sum.update(0 + integer);
        } else {
            sum.update(sum.value() + integer);
        }
        collector.collect(sum.value());
    }
}
