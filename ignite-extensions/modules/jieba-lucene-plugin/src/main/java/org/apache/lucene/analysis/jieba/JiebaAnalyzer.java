package org.apache.lucene.analysis.jieba;


import me.aias.jieba.JiebaSegmenter;
import me.aias.jieba.SegToken;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;

import java.util.Set;

public class JiebaAnalyzer extends Analyzer
{
    private boolean enablePorterStemming;
    private Set<String> filter;
    
    private JiebaTokenizerFactory jiebaTokenizerFactory;

    /**
     * @param filter    停用词
     * @param enablePorterStemming 是否分析词干（仅限英文）
     */
    public JiebaAnalyzer(Set<String> filter, boolean enablePorterStemming)
    {
        this.filter = filter;
        this.enablePorterStemming = enablePorterStemming;
    }

    /**
     * @param enablePorterStemming 是否分析词干.进行单复数,时态的转换
     */
    public JiebaAnalyzer(boolean enablePorterStemming)
    {
        this.enablePorterStemming = enablePorterStemming;
    }
    
    /**
     * @param JiebaTokenizerFactory
     */
    public JiebaAnalyzer(JiebaTokenizerFactory jiebaTokenizerFactory)
    {
        this.jiebaTokenizerFactory = jiebaTokenizerFactory;        
    }

    public JiebaAnalyzer()
    {
        super();
    }

    /**
     * 重载Analyzer接口，构造分词组件
     */
    @Override
    protected TokenStreamComponents createComponents(String fieldName)
    {
    	Tokenizer tokenizer;
    	if(jiebaTokenizerFactory!=null) {
    		jiebaTokenizerFactory.setEnableIndexMode(false);
    		tokenizer = jiebaTokenizerFactory.create();    		
    	}
    	else {
    		tokenizer = new JiebaTokenizer(new JiebaSegmenter(), JiebaSegmenter.SegMode.SEARCH, filter, enablePorterStemming);
    	}
        return new TokenStreamComponents(tokenizer);
    }
}
