package com.hankcs.lucene;

import com.hankcs.hanlp.HanLP;
import com.hankcs.hanlp.corpus.io.IOUtil;
import com.hankcs.hanlp.dictionary.stopword.StopWordDictionary;
import com.hankcs.hanlp.seg.Segment;
import com.hankcs.hanlp.seg.common.Term;
import com.hankcs.hanlp.tokenizer.TraditionalChineseTokenizer;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class HanLPTokenizerFactory extends TokenizerFactory
{
    private boolean enableIndexMode;
    private boolean enablePorterStemming;
    private boolean enableNumberQuantifierRecognize;
    private boolean enableCustomDictionary;
    private boolean enableCustomDictionaryForcing;
    private boolean enableTranslatedNameRecognize;
    private boolean enableJapaneseNameRecognize;
    private boolean enableOrganizationRecognize;
    private boolean enablePlaceRecognize;
    private boolean enableNameRecognize;
    private boolean enableTraditionalChineseMode;
    private String algorithm;
    private Set<String> stopWordDictionary;

    /**
     * 初始化工厂类
     *
     * @param args 通过这个Map保存xml中的配置项
     */
    public HanLPTokenizerFactory(Map<String, String> args)
    {
        super(args);
        enableIndexMode = getBoolean(args, "enableIndexMode", true);
        enablePorterStemming = getBoolean(args, "enablePorterStemming", false);
        enableNumberQuantifierRecognize = getBoolean(args, "enableNumberQuantifierRecognize", false);
        enableCustomDictionary = getBoolean(args, "enableCustomDictionary", true);
        enableCustomDictionaryForcing = getBoolean(args, "enableCustomDictionaryForcing", false);
        enableTranslatedNameRecognize = getBoolean(args, "enableTranslatedNameRecognize", false);
        enableJapaneseNameRecognize = getBoolean(args, "enableJapaneseNameRecognize", false);
        enableOrganizationRecognize = getBoolean(args, "enableOrganizationRecognize", false);
        enableNameRecognize = getBoolean(args, "enableNameRecognize", false);
        enablePlaceRecognize = getBoolean(args, "enablePlaceRecognize", false);
        enableTraditionalChineseMode = getBoolean(args, "enableTraditionalChineseMode", false);
        HanLP.Config.Normalization = getBoolean(args, "enableNormalization", HanLP.Config.Normalization);
        algorithm = getString(args, "algorithm", "viterbi");
        Set<String> customDictionaryPathSet = getSet(args, "customDictionaryPath");
        if (customDictionaryPathSet != null)
        {
            HanLP.Config.CustomDictionaryPath = customDictionaryPathSet.toArray(new String[0]);
        }
        String stopWordDictionaryPath = get(args, "stopWordDictionaryPath");
        if (stopWordDictionaryPath != null)
        {
            stopWordDictionary = new TreeSet<>();
            stopWordDictionary.addAll(IOUtil.readLineListWithLessMemory(stopWordDictionaryPath));
        }
        if (getBoolean(args, "enableDebug", false))
        {
            HanLP.Config.enableDebug();
        }
    }

    protected final String getString(Map<String, String> args, String name, String defaultVal)
    {
        String s = args.remove(name);
        return s == null ? defaultVal : s;
    }

    @Override
    public Tokenizer create(AttributeFactory factory)
    {
        Segment segment = HanLP.newSegment(algorithm).enableOffset(true).enableIndexMode(enableIndexMode)
                .enableNameRecognize(enableNameRecognize)
                .enableNumberQuantifierRecognize(enableNumberQuantifierRecognize)
                .enableCustomDictionary(enableCustomDictionary)
                .enableCustomDictionaryForcing(enableCustomDictionaryForcing)
                .enableTranslatedNameRecognize(enableTranslatedNameRecognize)
                .enableJapaneseNameRecognize(enableJapaneseNameRecognize)
                .enableOrganizationRecognize(enableOrganizationRecognize)
                .enablePlaceRecognize(enablePlaceRecognize);
        if (enableTraditionalChineseMode)
        {
            segment.enableIndexMode(false);
            Segment inner = segment;
            TraditionalChineseTokenizer.SEGMENT = inner;
            segment = new Segment()
            {
                @Override
                protected List<Term> segSentence(char[] sentence)
                {
                    List<Term> termList = TraditionalChineseTokenizer.segment(new String(sentence));
                    return termList;
                }
            };
        }

        return new HanLPTokenizer(segment
                , stopWordDictionary, enablePorterStemming);
    }
}
