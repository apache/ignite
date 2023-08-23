package org.apache.lucene.analysis.jieba;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;

public class JiebaIndexAnalyzerTest extends TestCase
{

    public void testCreateComponents() throws Exception
    {
        String text = "中华人民共和国很辽阔";
        text = "运动抽筋有哪些原因？如何处理>";
        for (int i = 0; i < text.length(); ++i)
        {
            System.out.print(text.charAt(i) + "" + i + " ");
        }
        System.out.println();
        Analyzer analyzer = new JiebaIndexAnalyzer();
        TokenStream tokenStream = analyzer.tokenStream("field", text);
        tokenStream.reset();
        while (tokenStream.incrementToken())
        {
            CharTermAttribute attribute = tokenStream.getAttribute(CharTermAttribute.class);
            // 偏移量
            OffsetAttribute offsetAtt = tokenStream.getAttribute(OffsetAttribute.class);
            // 距离
            PositionIncrementAttribute positionAttr = tokenStream.getAttribute(PositionIncrementAttribute.class);
            // 词性
            TypeAttribute typeAttr = tokenStream.getAttribute(TypeAttribute.class);
            System.out.printf("[%d:%d %d] %s/%s\n", offsetAtt.startOffset(), offsetAtt.endOffset(), positionAttr.getPositionIncrement(), attribute, typeAttr.type());
        }
    }
}