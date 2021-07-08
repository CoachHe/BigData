package com.coachhe.partitionCustom;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    Text k = new Text();

    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        // 138xxxxxxxx 2481 24861 3000
        // 138xxxxxxxx 318  32921 32893

        long sum_upFlow = 0;
        long sum_downFlow = 0;

        // 1 累加求和
        for (FlowBean flowBean : values) {
            sum_downFlow += flowBean.getDownFlow();
            sum_upFlow += flowBean.getUpFlow();
        }

        k.set(key);
        FlowBean v = new FlowBean(sum_upFlow, sum_downFlow);

        // 2 写出
        context.write(k, v);
    }
}
