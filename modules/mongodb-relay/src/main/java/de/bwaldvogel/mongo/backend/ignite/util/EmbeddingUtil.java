package de.bwaldvogel.mongo.backend.ignite.util;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

import org.apache.ignite.ml.math.primitives.vector.Vector;
import org.apache.ignite.ml.math.primitives.vector.impl.DenseVector;
import org.apache.ignite.ml.math.primitives.vector.impl.SparseVector;
import org.apache.lucene.analysis.jieba.JiebaAnalyzer;

import ai.djl.MalformedModelException;
import ai.djl.huggingface.tokenizers.Encoding;
import ai.djl.huggingface.tokenizers.HuggingFaceTokenizer;
import ai.djl.inference.Predictor;
import ai.djl.modality.nlp.preprocess.Tokenizer;
import ai.djl.repository.zoo.ModelNotFoundException;
import ai.djl.repository.zoo.ModelZoo;
import ai.djl.repository.zoo.ZooModel;
import ai.djl.sentencepiece.SpTextEmbedding;
import ai.djl.sentencepiece.SpTokenizer;
import ai.djl.translate.TranslateException;
import me.aias.example.utils.SentenceEncoder;

public class EmbeddingUtil {
	
	private static JiebaAnalyzer textAnalyzer = new JiebaAnalyzer();
	
	private static HashMap<String,Tokenizer> tokenizerCache = new HashMap<>();
	
	private static HashMap<String,Predictor> predictorCache = new HashMap<>(); 
	
	public static Tokenizer tokenizer(String name) {
		name = name.replace('\\', '/');
		Tokenizer tokenizer = tokenizerCache.get(name);
		if(tokenizer!=null) {
			return tokenizer;
		}
		
		if(name.endsWith(".model")) { // sentencepiece	
			try {
				Path modelFile = Paths.get(name);
				tokenizer = new SpTokenizer(modelFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		else {
			try {
				tokenizer = HuggingFaceTokenizer.newInstance(Paths.get(name));
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		    
		
		tokenizerCache.put(name, tokenizer);
		return tokenizer;
	}
	
	public static <I,O> Predictor<I,O> predictor(String name) {
		name = name.replace('\\', '/');
		Predictor<I,O> predictor = predictorCache.get(name);
		if(predictor!=null) {
			return predictor;
		}
		
		if(name.endsWith(".zip")) {
			SentenceEncoder sentenceEncoder = new SentenceEncoder();
			Path modelFile = Paths.get("models/sentencepiece.bpe.model");
			
			try {
				
				SpTextEmbedding processor = SpTextEmbedding.from(new SpTokenizer(modelFile));
				
				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(processor,name));
			            		  
				Predictor<String, float[]> predictorNew = model.newPredictor();
				
				predictor = (Predictor)predictorNew;
				
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ModelNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (MalformedModelException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		else {
			predictor = null;
		}		    
		
		predictorCache.put(name, predictor);
		return predictor;
	}
	
	public static Vector textTwoGramVec(String sentence) {
		SparseVector vec = new SparseVector(61580*3);
		File file = new File("models","perceiver-ar-xlnet-large");
		HuggingFaceTokenizer tokenizer = (HuggingFaceTokenizer)tokenizer(file.getPath());
		
		Encoding tokens = tokenizer.encode(sentence,false);
		int last = 0;
		for(long id: tokens.getIds()) {
			vec.set((int)id, 1.0);
			if(last>0) {
				vec.set((int)(61580+last+id), 0.5);
			}	
			// 是标点符号
			if(id>=59274 && id<59307) {
				last = 0;
			}
			else {
				last =(int)id;
			}
		}		
		
		return vec;
		
	}
	
	public static Vector textXlmVec(String sentence) {
		String root = "C:\\Code\\GIT\\AIAS-main\\2_nlp_sdks\\embedding\\sentence_encoder_100_sdk\\models\\";
		Predictor<String,float[]> predictor = predictor(root+"paraphrase-xlm-r-multilingual-v1.zip");
		
		try {
			float[] embedding = predictor.predict(sentence);
			DenseVector vec = new DenseVector(embedding);
			return vec;
		} catch (TranslateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
		
	}

}
