/*
 * <summary></summary>
 * <author>He Han</author>
 * <email>me@hankcs.com</email>
 * <create-date>2015/10/22 11:37</create-date>
 *
 * <copyright file="HighLighterDemo.java" company="码农场">
 * Copyright (c) 2008-2015, 码农场. All Right Reserved, http://www.hankcs.com/
 * This source is subject to Hankcs. Please contact Hankcs to get more information.
 * </copyright>
 */
package com.hankcs.lucene;

import junit.framework.TestCase;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.queryparser.classic.ParseException;
import org.apache.lucene.queryparser.classic.QueryParser;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.search.highlight.*;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

import java.io.IOException;

/**
 * 演示高亮搜索结果
 *
 * @author hankcs
 */
public class HighLighterTest extends TestCase
{

    public void testHightlight() throws Exception
    {
        // Lucene Document的主要域名
        String fieldName = "text";

        // 实例化Analyzer分词器
        Analyzer analyzer = new HanLPAnalyzer();

        Directory directory = null;
        IndexWriter iwriter;
        IndexReader ireader = null;
        IndexSearcher isearcher;
        try
        {
            //索引过程**********************************
            //建立内存索引对象
            directory = new RAMDirectory();

            //配置IndexWriterConfig
            IndexWriterConfig iwConfig = new IndexWriterConfig(analyzer);
            iwConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            iwriter = new IndexWriter(directory, iwConfig);
            {
                // 加入一个文档
                Document doc = new Document();
                doc.add(new TextField(fieldName, "\n返回值\r\n返回", Field.Store.YES));
                doc.add(new TextField("title", "测试回车换行符", Field.Store.YES));
                iwriter.addDocument(doc);
            }
            {
                // 再加入一个
                Document doc = new Document();
                doc.add(new TextField(fieldName, "\n\n   \n程序员\n\n喜欢黑夜", Field.Store.YES));
                doc.add(new TextField("title", "关于程序员", Field.Store.YES));
                iwriter.addDocument(doc);
            }
            iwriter.close();

            //搜索过程**********************************
            //实例化搜索器
            ireader = DirectoryReader.open(directory);
            isearcher = new IndexSearcher(ireader);

            String keyword = "返回";
            //使用QueryParser查询分析器构造Query对象
            QueryParser qp = new QueryParser(fieldName, analyzer);
            Query query = qp.parse(keyword);
            System.out.println("Query = " + query);

            //搜索相似度最高的5条记录
            TopDocs topDocs = isearcher.search(query, 5);
            System.out.println("命中：" + topDocs.totalHits);
            //输出结果
            ScoreDoc[] scoreDocs = topDocs.scoreDocs;

            for (int i = 0; i < Math.min(5, scoreDocs.length); ++i)
            {
                Document targetDoc = isearcher.doc(scoreDocs[i].doc);
                System.out.print(targetDoc.getField("title").stringValue());
                System.out.println(" , " + scoreDocs[i].score);

                String text = targetDoc.get(fieldName);
                System.out.println(displayHtmlHighlight(query, analyzer, fieldName, text, 200));
            }
        }
        catch (IOException | ParseException | InvalidTokenOffsetsException e)
        {
            e.printStackTrace();
        }
        finally
        {
            if (ireader != null)
            {
                try
                {
                    ireader.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
            if (directory != null)
            {
                try
                {
                    directory.close();
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 获取高亮显示结果的html代码
     *
     * @param query        查询
     * @param analyzer     分词器
     * @param fieldName    域名
     * @param fieldContent 域内容
     * @param fragmentSize 结果的长度（不含html标签长度）
     * @return 结果（一段html代码）
     * @throws IOException
     * @throws InvalidTokenOffsetsException
     */
    static String displayHtmlHighlight(Query query, Analyzer analyzer, String fieldName, String fieldContent, int fragmentSize) throws IOException, InvalidTokenOffsetsException
    {
        //创建一个高亮器
        Highlighter highlighter = new Highlighter(new SimpleHTMLFormatter("【", "】"), new QueryScorer(query));
        Fragmenter fragmenter = new SimpleFragmenter(fragmentSize);
        highlighter.setTextFragmenter(fragmenter);
        return highlighter.getBestFragment(analyzer, fieldName, fieldContent);
    }


    /**
     * 测试特殊分词器
     */
    public void testEmail()
    {
        // Lucene Document的主要域名
        String fieldName = "text";

        // 实例化Analyzer分词器
        Analyzer analyzer = new Analyzer()
        {
            @Override
            protected TokenStreamComponents createComponents(String s)
            {
                Tokenizer tokenizer = new HanLPTokenizer(new EmailSegment(), null, true);
                return new TokenStreamComponents(tokenizer);
            }
        };
        String keyword = "有事请发邮件：wxh192395009@qq.com";
        //使用QueryParser查询分析器构造Query对象
        QueryParser qp = new QueryParser(fieldName, analyzer);
        Query query = null;
        try
        {
            query = qp.parse(keyword);
        }
        catch (ParseException e)
        {
            e.printStackTrace();
        }
        System.out.println("Query = " + query);
    }
}
