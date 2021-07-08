package com.coachhe.WritableComparable.全排序;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowCountReducer extends Reducer<FlowBean, Text, Text, FlowBean> {

    @Override
    protected void reduce(FlowBean key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        // bean 138XXXX
        for (Text phone_num : values) {
            context.write(phone_num, key);
        }
    }
}
