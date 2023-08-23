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
import me.aias.example.utils.ParaphraseSentenceEncoder;
import me.aias.example.utils.SentenceEncoder;

public class EmbeddingUtil {	
	
	
	private static HashMap<String,Tokenizer> tokenizerCache = new HashMap<>();
	
	private static HashMap<String,Predictor> predictorCache = new HashMap<>(); 
	
	public static Tokenizer tokenizer(Path name) {		
		Tokenizer tokenizer = tokenizerCache.get(name.toString());
		if(tokenizer!=null) {
			return tokenizer;
		}
		
		if(name.toString().endsWith(".model")) { // is sentencepiece model	
			try {				
				tokenizer = new SpTokenizer(name);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
		}
		else {
			try {
				tokenizer = HuggingFaceTokenizer.newInstance(name);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}		    
		
		tokenizerCache.put(name.toString(), tokenizer);
		return tokenizer;
	}
	
	public static Predictor<String, float[]> predictor(Path name) {
		
		Predictor<String, float[]> predictor = predictorCache.get(name.toString());
		if(predictor!=null) {
			return predictor;
		}
		Tokenizer spTokenizer = null;
		Path modelPath = name;		
		Path sentencepieceModelFile = modelPath.resolve("sentencepiece.bpe.model");
		if(sentencepieceModelFile.toFile().exists()) {
			spTokenizer = tokenizer(sentencepieceModelFile);			
		}
				
		if(spTokenizer!=null && spTokenizer instanceof SpTokenizer) {
			SentenceEncoder sentenceEncoder = new SentenceEncoder();
			try {
				
				SpTextEmbedding processor = SpTextEmbedding.from((SpTokenizer)spTokenizer);
				
				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(processor,name.toString()));
			            		  
				Predictor<String, float[]> predictorNew = model.newPredictor();
				
				predictor = predictorNew;
				
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
			ParaphraseSentenceEncoder sentenceEncoder = new ParaphraseSentenceEncoder();
			try {		
				
				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(name));
			            		  
				Predictor<String, float[]> predictorNew = model.newPredictor();
				
				predictor = predictorNew;
				
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
		
		predictorCache.put(name.toString(), predictor);
		return predictor;
	}
	
	/**
	 *  remote or local zip model	
	 * @param name
	 * @return
	 */
	public static Predictor<String, float[]> predictor(String name) {
		
		Predictor<String, float[]> predictor = predictorCache.get(name.toString());
		if(predictor!=null) {
			return predictor;
		}
		
		if(name.toString().endsWith(".zip")) {
			ParaphraseSentenceEncoder sentenceEncoder = new ParaphraseSentenceEncoder();
			try {		
				
				ZooModel<String, float[]> model = ModelZoo.loadModel(sentenceEncoder.criteria(name));
			            		  
				Predictor<String, float[]> predictorNew = model.newPredictor();
				
				predictor = predictorNew;
				
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
	
	public static Vector textTwoGramVec(String sentence,String modelId) {		
		Path file = Paths.get(modelId);
		HuggingFaceTokenizer tokenizer = (HuggingFaceTokenizer)tokenizer(file);
		// perceive-xlm-large
		int vocbSize = 61580;
		SparseVector vec = new SparseVector(vocbSize*2);
		Encoding tokens = tokenizer.encode(sentence,false);
		int last = 0;
		for(long id: tokens.getIds()) {
			vec.set((int)id, 1.0);
			if(last>0) {
				long hashed = (last*37+id) % vocbSize;
				vec.set((int)(vocbSize+hashed), 0.5);
			}	
			// 是标点符号，特殊字符
			if(id>=59274) {
				last = 0;
			}
			else {
				last =(int)id;
			}
		}		
		
		return vec;
		
	}
	
	public static Vector textXlmVec(String sentence,String modelId) {
		Predictor<String,float[]> predictor;
		if(modelId.indexOf("://")>1) {
			predictor = predictor(modelId);
		}
		else {
			Path modelPath = Paths.get(modelId);		
			predictor = predictor(modelPath);
		}
		
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
