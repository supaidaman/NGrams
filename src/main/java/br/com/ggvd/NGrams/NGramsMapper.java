package br.com.ggvd.NGrams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class NGramsMapper extends Mapper<Object, Text, Text, IntWritable> {

    private final IntWritable NUMBER = new IntWritable(1);
    private final Text word = new Text();

    @Override
    public void map(Object key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        Integer nGramsSize = Integer.parseInt(conf.get("ngramSize"));
        Integer minimumCountSize = Integer.parseInt(conf.get("minimumCountSize"));

        String line = value.toString();
        StringTokenizer st = new StringTokenizer(line, " ");
        ArrayList<String> myArray = new ArrayList<String>();
        while (st.hasMoreTokens()){
            myArray.add(st.nextToken());
        }
        try {
            var result = NGramImpl.doNGrams(nGramsSize, minimumCountSize, myArray);

            result.forEach((k, v) ->
            {
                word.set(k);
                NUMBER.set(v);
                try {
                    context.write(word, NUMBER);
                }
                catch (IOException | InterruptedException unhandledException)
                { System.out.println("deu ruim no write");

                }
            });
        }
        catch (Exception e)
        {
            System.out.println("deu ruim");
        }

    }
}
