package br.com.ggvd.NGrams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.StringTokenizer;

import org.apache.commons.lang.UnhandledException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileSplit;
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
        ArrayList<String> stringTokens = new ArrayList<String>();
        while (st.hasMoreTokens()){
            stringTokens.add(st.nextToken());
        }

      //  String fileName = ((FileSplit) context.getInputSplit()).getPath().getName(); //dunno how to use now...


        try {
            var result = NGramImpl.doNGrams(nGramsSize, stringTokens);
            result.forEach(ngram ->
            {
                word.set(ngram);
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
