package com.hankcs.lucene;

import com.hankcs.hanlp.HanLP;
import junit.framework.TestCase;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

public class HanLPTokenizerTest extends TestCase
{
    Tokenizer tokenizer;

    @Override
    public void setUp() throws Exception
    {
        tokenizer = new HanLPTokenizer(HanLP.newSegment()
                                               .enableJapaneseNameRecognize(true)
                                               .enableIndexMode(true), null, false);
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
            TypeAttribute typeAttr = tokenizer.getAttribute(TypeAttribute.class);
            System.out.printf("[%d:%d %d] %s/%s\n", offsetAtt.startOffset(), offsetAtt.endOffset(), positionAttr.getPositionIncrement(), attribute, typeAttr.type());
        }
    }

    public void testMultiText() throws Exception
    {
        String[] sentences = new String[]{
                "中华人民共和国",
                "地大物博"
        };
        tokenizer = new HanLPTokenizer(HanLP.newSegment()
                                               .enableJapaneseNameRecognize(true)
                                               .enableIndexMode(true), null, false);
        for (String sentence : sentences)
        {
            tokenizer.setReader(new StringReader(sentence));
            tokenizer.reset();
            testIncrementToken();
            tokenizer.close();
        }
    }

    public void testPinyinTokenFilter() throws Exception
    {
        Map<String, String> args = new HashMap<>();
        args.put("original", "true");
        args.put("pinyin", "false");
        args.put("pinyinFirstChar", "true");
        HanLPPinyinTokenFilterFactory factory = new HanLPPinyinTokenFilterFactory(args);
        TokenStream tokenStream = factory.create(tokenizer);
        while (tokenStream.incrementToken())
        {
            CharTermAttribute attribute = tokenizer.getAttribute(CharTermAttribute.class);
            // 偏移量
            OffsetAttribute offsetAtt = tokenizer.getAttribute(OffsetAttribute.class);
            // 距离
            PositionIncrementAttribute positionAttr = tokenizer.getAttribute(PositionIncrementAttribute.class);
            // 词性
            TypeAttribute typeAttr = tokenizer.getAttribute(TypeAttribute.class);
            System.out.printf("[%d:%d %d] %s/%s\n", offsetAtt.startOffset(), offsetAtt.endOffset(), positionAttr.getPositionIncrement(), attribute, typeAttr.type());
        }
    }
}