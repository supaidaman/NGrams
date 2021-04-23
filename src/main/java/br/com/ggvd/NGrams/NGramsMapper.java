package br.com.ggvd.NGrams;

import java.io.IOException;
import java.util.ArrayList;
import java.util.StringTokenizer;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.Mapper;


public class NGramsMapper extends Mapper<Object, Text, Text, NGramResultWriteable> {

    private final  NGramResultWriteable NGRAM = new NGramResultWriteable();
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

       String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();

        //foi necessário colocar o trycatch pois meu compilador estava reclamando
        try {
            var result = NGramImpl.doNGrams(nGramsSize, stringTokens);
            result.forEach(ngram ->
            {
                NGRAM.setnGramCount(new IntWritable(1));
                NGRAM.setFileName(new Text(fileName));
                word.set(ngram);
                try {
                    context.write(word, NGRAM);
                }
                catch (IOException | InterruptedException unhandledException)
                { System.out.println("erro ao escrever");

                }
            });

        }
        catch (Exception e)
        {
            System.out.println("Ocorreu um problema na execução");
        }

    }
}
