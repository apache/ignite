package org.apache.lucene.analysis.jieba;



import me.aias.jieba.JiebaSegmenter;
import me.aias.jieba.SegToken;

import org.apache.commons.lang3.StringUtils;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Tokenizer，抄袭ansj的
 */
public class JiebaTokenizer extends Tokenizer
{
    // 当前词
    private final CharTermAttribute termAtt = addAttribute(CharTermAttribute.class);
    // 偏移量
    private final OffsetAttribute offsetAtt = addAttribute(OffsetAttribute.class);
    // 距离
    private final PositionIncrementAttribute positionAttr = addAttribute(PositionIncrementAttribute.class);
   

    private SegmentWrapper segment;
    private Set<String> filter;
    private boolean enablePorterStemming;
    private final PorterStemmer stemmer = new PorterStemmer();

    /**
     * 单文档当前所在的总offset，当reset（切换multi-value fields中的value）的时候不清零，在end（切换field）时清零
     */
    private int totalOffset = 0;

    /**
     * @param segment              Jieba中的某个分词器
     * @param filter               停用词
     * @param enablePorterStemming 英文原型转换
     */
    public JiebaTokenizer(JiebaSegmenter segment, JiebaSegmenter.SegMode segMode, Set<String> filter, boolean enablePorterStemming)
    {
        super();
        this.segment = new SegmentWrapper(input, segment, segMode);
        this.filter = new HashSet<String>();
        
        if (filter != null && filter.size() > 0)
        {           
            this.filter.addAll(filter);
        }
        this.enablePorterStemming = enablePorterStemming;
    }

    @Override
    final public boolean incrementToken() throws IOException
    {
        clearAttributes();
        int position = 0;
        SegToken term;
        boolean un_increased = true;
        do
        {
            term = segment.next();
            if (term == null)
            {
                break;
            }
            if (StringUtils.isBlank(term.word)) // 过滤掉空白符，提高索引效率
            {
                continue;
            }
            if (enablePorterStemming && StringUtils.isAlphanumeric(term.word))
            {
                term.word = stemmer.stem(term.word);
            }

            if (filter != null && filter.contains(term.word))
            {
                continue;
            }
            else
            {
                ++position;
                un_increased = false;
            }
        }
        while (un_increased);

        if (term != null)
        {
            positionAttr.setPositionIncrement(position);
            termAtt.setEmpty().append(term.word);
            offsetAtt.setOffset(correctOffset(totalOffset + term.startOffset),
                                correctOffset(totalOffset + term.endOffset));
           
            return true;
        }
        else
        {
            totalOffset += segment.offset;
            return false;
        }
    }

    @Override
    public void end() throws IOException
    {
        super.end();
        offsetAtt.setOffset(totalOffset, totalOffset);
        totalOffset = 0;
    }

    /**
     * 必须重载的方法，否则在批量索引文件时将会导致文件索引失败
     */
    @Override
    public void reset() throws IOException
    {
        super.reset();
        segment.reset(new BufferedReader(this.input));
    }

}
