package com.hankcs.lucene;

import com.hankcs.hanlp.corpus.tag.Nature;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * hanlp-lucene-plugin
 * com.hankcs.lucene
 * Created by HEZHILONG on 2018-08-28.
 */
public class EmailSegment extends Segment
{
    private static final Pattern emailPattern = Pattern.compile("(\\w+(?:[-+.]\\w+)*)@(\\w+(?:[-.]\\w+)*\\.\\w+(?:[-.]\\w+)*)");

    @Override
    protected List<Term> segSentence(char[] chars)
    {
        String text = new String(chars);
        final Matcher matcher = emailPattern.matcher(text);
        List<Term> resultList = new ArrayList<>();
        while (matcher.find())
        {
            final int start = matcher.start();
            resultList.add(new Term(matcher.group(), Nature.nx)
            {{
                offset = start;
            }});
            final String uName = matcher.group(1);

            resultList.add(new Term(uName, Nature.nx)
            {{
                offset = start;
            }});
            resultList.add(new Term(matcher.group(2), Nature.nx)
            {{
                offset = start;
            }});
        }
        return resultList;
    }
}
