package com.hankcs.lucene;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.util.TokenFilterFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class HanLPPinyinTokenFilterFactory extends TokenFilterFactory {
    private boolean original;
    private boolean pinyin;
    private boolean pinyinFirstChar;

    /**
     * 初始化工厂类
     *
     * @param args 通过这个Map保存xml中的配置项
     */
    public HanLPPinyinTokenFilterFactory(Map<String, String> args) {
        super(args);
        original = getBoolean(args, "original", true);
        pinyin = getBoolean(args, "pinyin", true);
        pinyinFirstChar = getBoolean(args, "pinyinFirstChar", true);
    }

    @Override
    public TokenStream create(TokenStream input) {
        List<HanLPPinyinConverter> converters = new ArrayList<>();
        if (pinyin) {
            converters.add(new HanLPPinyinConverter.ToPinyinString());
        }
        if (pinyinFirstChar) {
            converters.add(new HanLPPinyinConverter.ToPinyinFirstCharString());
        }
        return new HanLPPinyinTokenFilter(input, original, converters);
    }
}
