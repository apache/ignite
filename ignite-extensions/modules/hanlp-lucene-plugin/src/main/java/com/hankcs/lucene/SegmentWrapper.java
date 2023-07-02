/*
 * <summary></summary>
 * <author>hankcs</author>
 * <email>me@hankcs.com</email>
 * <create-date>2015/10/6 18:51</create-date>
 *
 * <copyright file="SegmentWrapper.java">
 * Copyright (c) 2003-2015, hankcs. All Right Reserved, http://www.hankcs.com/
 * </copyright>
 */
package com.hankcs.lucene;

import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

import java.io.IOException;
import java.io.Reader;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

/**
 * 将分词器包装起来，每次输出一个token
 *
 * @author hankcs
 */
public class SegmentWrapper
{
    /**
     * 输入
     */
    private Reader input;
    /**
     * 分词器
     */
    private Segment segment;
    /**
     * 分词结果
     */
    private Iterator<Term> iterator;
    /**
     * term的偏移量，由于wrapper是按行读取的，必须对term.offset做一个校正
     */
    int offset;
    /**
     * 缓冲区大小
     */
    private static final int BUFFER_SIZE = 512;
    /**
     * 缓冲区
     */
    private char[] buffer = new char[BUFFER_SIZE];
    /**
     * 缓冲区未处理的下标
     */
    private int remainSize = 0;


    /**
     * 句子分隔符
     */
    private static final Set<Character> delimiterCharSet = new HashSet<Character>()
    {{
        add('\r');
        add('\n');
        add(';');
        add('；');
        add('。');
        add('!');
        add('！');
    }};

    public SegmentWrapper(Reader reader, Segment segment)
    {
        this.input = reader;
        this.segment = segment;
    }

    /**
     * 重置分词器
     *
     * @param reader
     */
    public void reset(Reader reader)
    {
        input = reader;
        offset = 0;
        iterator = null;
    }

    public Term next() throws IOException
    {
        if (iterator != null && iterator.hasNext()) return iterator.next();
        String line = readLine();
        if (line == null) return null;
        List<Term> termList = segment.seg(line);
        if (termList.size() == 0) return null;
        for (Term term : termList)
        {
            term.offset += offset;
        }
        offset += line.length();
        iterator = termList.iterator();
        return iterator.next();
    }

    private String readLine() throws IOException
    {
        int offset = 0;
        int length = BUFFER_SIZE;
        if (remainSize > 0)
        {
            offset = remainSize;
            length -= remainSize;
        }
        int n = input.read(buffer, offset, length);
        if (n < 0)
        {
            if (remainSize != 0)
            {
                String lastLine = new String(buffer, 0, remainSize);
                remainSize = 0;
                return lastLine;
            }
            return null;
        }
        n += offset;

        int eos = lastIndexOfEos(buffer, n);
        String line = new String(buffer, 0, eos);
        remainSize = n - eos;
        System.arraycopy(buffer, eos, buffer, 0, remainSize);
        return line;
    }

    private int lastIndexOfEos(char[] buffer, int length)
    {
        for (int i = length - 1; i > 0; i--)
        {
            if (delimiterCharSet.contains(buffer[i]))
            {
                return i + 1;
            }
        }
        return length;
    }
}
