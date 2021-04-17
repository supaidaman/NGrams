package br.com.ggvd.NGrams;

import java.util.*;

public class NGramImpl {


    public static HashMap<String,Integer> doNGrams(Integer size, Integer limit, ArrayList<String> sentence)
            throws Exception {

        if(size <= 0)
            throw new Exception("Tamanho Inválido");

        String empty = " ";
        ArrayList<String> ngrams = new ArrayList<String>();
        HashMap<String,Integer> vocabularyCount = new HashMap<String,Integer>();
        Set<String> vocabulary = new HashSet<String>(sentence);


        for(int i =0; i < sentence.size() - size + 1; i++)
        {
            String palavraNGram = sentence.get(i);

            for(int j = i + 1; j < (i + size); j++)
            {
                palavraNGram+= empty + sentence.get(j);
            }
            if(vocabularyCount.containsKey(palavraNGram))
            {
               Integer value = vocabularyCount.get(palavraNGram);
               value= value + 1;
               vocabularyCount.replace(palavraNGram,value);
            }
            else
            {
                vocabularyCount.put(palavraNGram,1);
            }

        }
        vocabularyCount.entrySet().removeIf(e -> e.getValue() < limit);

        return  vocabularyCount;
    }
}
