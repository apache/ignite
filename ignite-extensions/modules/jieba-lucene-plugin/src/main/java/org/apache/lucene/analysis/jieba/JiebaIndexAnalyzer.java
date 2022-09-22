package org.apache.lucene.analysis.jieba;


import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

import me.aias.jieba.JiebaSegmenter;

import java.util.Set;

public class JiebaIndexAnalyzer extends Analyzer
{

    private boolean pstemming;
    private Set<String> filter;
    
    private JiebaTokenizerFactory jiebaTokenizerFactory;
    

    /**
     * @param filter    停用词
     * @param pstemming 是否分析词干
     */
    public JiebaIndexAnalyzer(Set<String> filter, boolean pstemming)
    {
        this.filter = filter;
        this.pstemming = pstemming;
    }

    /**
     * @param pstemming 是否分析词干.进行单复数,时态的转换
     */
    public JiebaIndexAnalyzer(boolean pstemming)
    {
        this.pstemming = pstemming;
    }

    public JiebaIndexAnalyzer()
    {
        super();
    }

    @Override
    protected TokenStreamComponents createComponents(String fieldName)
    {
    	Tokenizer tokenizer;
    	if(jiebaTokenizerFactory!=null) {
    		jiebaTokenizerFactory.setEnableIndexMode(true);
    		tokenizer = jiebaTokenizerFactory.create();
    	}
    	else {
    		tokenizer = new JiebaTokenizer(new JiebaSegmenter(), JiebaSegmenter.SegMode.INDEX, filter, pstemming);
    	}
        return new TokenStreamComponents(tokenizer);
    }
}
