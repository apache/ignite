package com.hankcs.lucene;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.analysis.tokenattributes.OffsetAttribute;
import org.apache.lucene.analysis.tokenattributes.PositionIncrementAttribute;
import org.apache.lucene.analysis.tokenattributes.TypeAttribute;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.RAMDirectory;

import java.io.File;
import java.io.StringReader;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;
import java.util.TreeMap;

public class HanLPAnalyzerTest extends TestCase
{

    public void testCreateComponents() throws Exception
    {
        String text = "中华人民共和国很辽阔";
        for (int i = 0; i < text.length(); ++i)
        {
            System.out.print(text.charAt(i) + "" + i + " ");
        }
        System.out.println();
        Analyzer analyzer = new HanLPAnalyzer();
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

    public void testIndexAndSearch() throws Exception
    {
        Analyzer analyzer = new HanLPAnalyzer();////////////////////////////////////////////////////
        IndexWriterConfig config = new IndexWriterConfig(analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE);
        Directory directory = new RAMDirectory();
        IndexWriter indexWriter = new IndexWriter(directory, config);

        Document document = new Document();
        document.add(new TextField("content", "[新闻]服务大众。", Field.Store.YES));
        indexWriter.addDocument(document);

        document = new Document();
        document.add(new TextField("content", "[经济学]商品和服务", Field.Store.YES));
        indexWriter.addDocument(document);

        document = new Document();
        document.add(new TextField("content", "[服装店]和服的价格是每镑15便士", Field.Store.YES));
        indexWriter.addDocument(document);

        indexWriter.commit();
        indexWriter.close();

        IndexReader ireader = DirectoryReader.open(directory);
        IndexSearcher isearcher = new IndexSearcher(ireader);
        QueryParser parser = new QueryParser("content", analyzer);
        Query query = parser.parse("和服");
        ScoreDoc[] hits = isearcher.search(query, 300000).scoreDocs;
        assertEquals(1, hits.length);
        for (ScoreDoc scoreDoc : hits)
        {
            Document targetDoc = isearcher.doc(scoreDoc.doc);
            System.out.println(targetDoc.getField("content").stringValue());
        }
    }

    public void testIssue() throws Exception
    {
        Map<String, String> args = new TreeMap<>();
        args.put("enableTraditionalChineseMode", "true");
        args.put("enableNormalization", "true");
        HanLPTokenizerFactory factory = new HanLPTokenizerFactory(args);
        Tokenizer tokenizer = factory.create();
        String text = "會辦台星保證最低價的原因？";

        tokenizer.setReader(new StringReader(text));
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