package br.com.ggvd.NGrams;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class NGrams {
    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {


        Path inputPath = new Path(args[2]);
        Path outputDir = new Path(args[3]);



        Configuration conf = new Configuration(true);
        conf.set("ngramSize", args[0]);
        conf.set("minimumCountSize", args[1]);

        
        // Criar o job
        Job job = new Job(conf, "NGrams");
        job.setJarByClass(NGrams.class);

        // Definir classes para Map e Reduce
        job.setMapperClass(NGramsMapper.class);
        job.setReducerClass(NGramsReducer.class);
        job.setNumReduceTasks(1);


        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NGramResultWriteable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputPath);



        FileOutputFormat.setOutputPath(job, outputDir);



        FileSystem hdfs = FileSystem.get(conf);
        if (hdfs.exists(outputDir))
            hdfs.delete(outputDir, true);


        int code = job.waitForCompletion(true) ? 0 : 1;
        System.exit(code);

    }
}
