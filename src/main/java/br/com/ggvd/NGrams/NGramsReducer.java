package br.com.ggvd.NGrams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class NGramsReducer extends
        Reducer<Text, IntWritable, Text, IntWritable>  {

    @Override
    public void reduce(Text text, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int sum = 0;
        for (IntWritable value : values) {
            sum += value.get();
        }
        Integer minimumCountSize = Integer.parseInt(conf.get("minimumCountSize"));
        if(sum >= minimumCountSize)
            context.write(text, new IntWritable(sum));
    }

}
