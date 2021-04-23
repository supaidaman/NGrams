package br.com.ggvd.NGrams;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;


public class NGramsReducer extends
        Reducer<Text, NGramResultWriteable, Text, Text>  {

    @Override
    public void reduce(Text text, Iterable<NGramResultWriteable> values, Context context)
            throws IOException, InterruptedException {

        Configuration conf = context.getConfiguration();
        int sum = 0;
        String fileName = new String();
        for (NGramResultWriteable value : values) {
            sum += value.getnGramCount().get();
            fileName = value.getFileName().toString();
        }
        System.out.println("Aqui estou com" + String.valueOf(sum));
        Integer minimumCountSize = Integer.parseInt(conf.get("minimumCountSize"));
        if(sum >= minimumCountSize)
            context.write(text, new NGramResultWriteable(sum,fileName).toText());
    }

}
