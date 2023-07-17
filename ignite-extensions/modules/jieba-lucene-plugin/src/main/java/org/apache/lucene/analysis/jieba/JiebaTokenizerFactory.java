package org.apache.lucene.analysis.jieba;


import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.util.TokenizerFactory;
import org.apache.lucene.util.AttributeFactory;
import org.apache.lucene.util.IOUtils;

import me.aias.jieba.JiebaSegmenter;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

public class JiebaTokenizerFactory extends TokenizerFactory
{
    private boolean enableIndexMode;
	private boolean enablePorterStemming;
    private boolean enableNumberQuantifierRecognize;    
    private boolean enableTranslatedNameRecognize;
    private boolean enableJapaneseNameRecognize;
    private boolean enableOrganizationRecognize;
    private boolean enablePlaceRecognize;
    private boolean enableNameRecognize;
    private boolean enableTraditionalChineseMode;
    private boolean enableNormalization;
    
   
    private Set<String> stopWordDictionary;
    
    private Set<String> customDictionaryPathSet;

    /**
     * 初始化工厂类
     *
     * @param args 通过这个Map保存xml中的配置项
     * @throws IOException 
     */
    public JiebaTokenizerFactory(Map<String, String> args) throws IOException
    {
        super(args);
        enableIndexMode = getBoolean(args, "enableIndexMode", true);
        enablePorterStemming = getBoolean(args, "enablePorterStemming", false);
        enableNumberQuantifierRecognize = getBoolean(args, "enableNumberQuantifierRecognize", false);        
        enableTranslatedNameRecognize = getBoolean(args, "enableTranslatedNameRecognize", false);
        enableJapaneseNameRecognize = getBoolean(args, "enableJapaneseNameRecognize", false);
        enableOrganizationRecognize = getBoolean(args, "enableOrganizationRecognize", false);
        enableNameRecognize = getBoolean(args, "enableNameRecognize", false);
        enablePlaceRecognize = getBoolean(args, "enablePlaceRecognize", false);
        enableTraditionalChineseMode = getBoolean(args, "enableTraditionalChineseMode", false);
        enableNormalization = getBoolean(args, "enableNormalization", true);
       
        customDictionaryPathSet = getSet(args, "customDictionaryPath");
        
        String stopWordDictionaryPath = get(args, "stopWordDictionaryPath");
        if (stopWordDictionaryPath != null)
        {
            stopWordDictionary = new TreeSet<>();
            
            Path path = Paths.get(stopWordDictionaryPath);
            Reader reader = IOUtils.getDecodingReader(new FileInputStream(stopWordDictionaryPath), java.nio.charset.StandardCharsets.UTF_8);
            BufferedReader lineReader = new BufferedReader(reader);
            String line = null;
            while((line=lineReader.readLine())!=null) {
            	stopWordDictionary.add(line);
            }
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
    	JiebaSegmenter segment = new JiebaSegmenter();
    	JiebaSegmenter.SegMode segMode = JiebaSegmenter.SegMode.SEARCH;
    	if(enableIndexMode) {
    		segMode = JiebaSegmenter.SegMode.INDEX;
    	}
    	/**
    		segment
                .enableNameRecognize(enableNameRecognize)
                .enableNumberQuantifierRecognize(enableNumberQuantifierRecognize)               
                .enableTranslatedNameRecognize(enableTranslatedNameRecognize)
                .enableJapaneseNameRecognize(enableJapaneseNameRecognize)
                .enableOrganizationRecognize(enableOrganizationRecognize)
                .enablePlaceRecognize(enablePlaceRecognize);
                
        */
        if (customDictionaryPathSet!=null)
        {
        	for(String pathStr: customDictionaryPathSet) {
        		Path path = Paths.get(pathStr);
        		segment.initUserDict(path);
        	}
        }       
        return new JiebaTokenizer(segment, segMode, stopWordDictionary, enablePorterStemming);
    }
    
    public boolean isEnableIndexMode() {
		return enableIndexMode;
	}

	public void setEnableIndexMode(boolean enableIndexMode) {
		this.enableIndexMode = enableIndexMode;
	}
}
