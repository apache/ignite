package org.apache.lucene.analysis.jieba;


import junit.framework.TestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import me.aias.jieba.JiebaSegmenter;
import me.aias.jieba.SegToken;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class JiebaTokenizerTest extends TestCase
{
    Tokenizer tokenizer;

    @Override
    public void setUp() throws Exception
    {
        tokenizer = new JiebaTokenizer(new JiebaSegmenter(), JiebaSegmenter.SegMode.SEARCH, null, false);
        tokenizer.setReader(new StringReader("林志玲亮相网友:确定不是波多野结衣？"));
        tokenizer.reset();
    }

    public void testIncrementToken() throws Exception
    {
        while (tokenizer.incrementToken())
        {
            CharTermAttribute attribute = tokenizer.getAttribute(CharTermAttribute.class);
            // 偏移量
            OffsetAttribute offsetAtt = tokenizer.getAttribute(OffsetAttribute.class);
            // 距离
            PositionIncrementAttribute positionAttr = tokenizer.getAttribute(PositionIncrementAttribute.class);
            // 词性
            
            System.out.printf("[%d:%d %d] %s\n", offsetAtt.startOffset(), offsetAtt.endOffset(), positionAttr.getPositionIncrement(), attribute);
        }
    }

    public void testMultiText() throws Exception
    {
        String[] sentences = new String[]{
                "中华人民共和国",
                "地大物博"
        };
       
        for (String sentence : sentences)
        {
        	tokenizer.close();
            tokenizer.setReader(new StringReader(sentence));
            tokenizer.reset();
            testIncrementToken();
            tokenizer.close();
        }
    }

   
}