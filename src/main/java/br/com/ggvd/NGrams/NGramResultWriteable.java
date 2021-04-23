package br.com.ggvd.NGrams;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

//Classe criada para ser meu proprio conjunto de dados de retorno
public class NGramResultWriteable implements WritableComparable {

    private IntWritable nGramCount;
    private Text fileName;


    public IntWritable getnGramCount() {
        return nGramCount;
    }

    public void setnGramCount(IntWritable nGramCount) {
        this.nGramCount = nGramCount;
    }

    public Text getFileName() {
        return fileName;
    }

    public void setFileName(Text fileName) {
        this.fileName = fileName;
    }


   public NGramResultWriteable()
   {
       nGramCount = new IntWritable(0);
       fileName = new Text();
   }
   public NGramResultWriteable(int intCount, String fileNameString)
   {
       nGramCount = new IntWritable(intCount);
       fileName = new Text(fileNameString);
   }
    @Override
    public void write(DataOutput dataOutput) throws IOException {
        nGramCount.write(dataOutput);
        fileName.write(dataOutput);
    }

    @Override
    public void readFields(DataInput dataInput) throws IOException {
       nGramCount.readFields(dataInput);
       fileName.readFields(dataInput);

    }

    public static NGramResultWriteable read(DataInput in) throws IOException {
        NGramResultWriteable w = new NGramResultWriteable();
        w.readFields(in);
        return w;
    }

    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append(nGramCount.toString()).append(" ").append(fileName.toString());
        return  builder.toString();
    }

    public Text toText() {
        StringBuilder builder = new StringBuilder();
        builder.append(nGramCount.toString()).append(" ").append(fileName.toString());
        return new Text(builder.toString());
    }

    @Override
    public int compareTo(Object o) {
        NGramResultWriteable n = (NGramResultWriteable)o;
        int integerCompare = this.nGramCount.compareTo(n.nGramCount);
        if(integerCompare == 0) {
            return this.fileName.compareTo(n.fileName);
        }
        else
        {return integerCompare;}
    }
}
