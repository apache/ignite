package com.hankcs.lucene;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.analysis.util.TokenizerFactory;

import java.io.StringReader;
import java.util.Map;
import java.util.TreeMap;

public class HanLPTokenizerFactoryTest extends TestCase
{

    public void testCreate() throws Exception
    {
        Map<String, String> args = new TreeMap<>();
        args.put("enableTraditionalChineseMode", "true");
        TokenizerFactory factory = new HanLPTokenizerFactory(args);
        Tokenizer tokenizer = factory.create(null);

        tokenizer.setReader(new StringReader("大衛貝克漢不僅僅是名著名球員，球場以外，其妻為前" +
                                                     "辣妹合唱團成員維多利亞·碧咸，亦由於他擁有" +
                                                     "突出外表、百變髮型及正面的形象，以至自己" +
                                                     "品牌的男士香水等商品，及長期擔任運動品牌" +
                                                     "Adidas的代言人，因此對大眾傳播媒介和時尚界" +
                                                     "等方面都具很大的影響力，在足球圈外所獲得的" +
                                                     "認受程度可謂前所未見。"));
        tokenizer.reset();
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
}