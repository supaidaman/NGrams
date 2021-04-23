package br.com.ggvd.NGrams;

import java.util.*;
//Implementação do NGram
public class NGramImpl {


    public static ArrayList<String> doNGrams(Integer size,ArrayList<String> palavras)
            throws Exception {

        if(size <= 0)
            throw new Exception("Tamanho Inválido");
        ArrayList<String> ngrams = new ArrayList<String>();
        for(int i =0; i < palavras.size() - size + 1; i++)
        {
            String palavraNGram = palavras.get(i);

            for(int j = i + 1; j < (i + size); j++)
            {
                palavraNGram+= " " + palavras.get(j);
            }
            ngrams.add(palavraNGram);

        }


        return  ngrams;
    }
}
