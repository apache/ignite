package org.elasticsearch.relay.postprocess;

import java.util.Set;
import java.util.logging.Logger;

import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.SensitivewordFilter;

import com.fasterxml.jackson.databind.node.ObjectNode;


/** 过滤敏感词
 *  如果是标题，过滤回车符，截断过长的标题
 *  如果含有敏感词，返回空文本。
 * @author WBPC1158
 *
 */
public class ContentPostProcessor implements IPostProcessor {
	static Logger fLogger = Logger.getLogger(ContentPostProcessor.class.getName());
	
	private  String CONTENT_FIELD = "content";
	private  String TITLE_FIELD = "title";

	


	private  int MAX_FIELD_LENGTH = 255;
	
	private String sensitiveWordFile = "sensitive_words.txt";
	
	private SensitivewordFilter sensitivewordFilter;
	
	
	
	private Set<String>  typeSet= null;
	
	public Set<String> getTypeSet() {
		return typeSet;
	}

	public void setTypeSet(Set<String> typeSet) {
		this.typeSet = typeSet;
	}


	public String getSensitiveWordFile() {
		return sensitiveWordFile;
	}

	public void setSensitiveWordFile(String sensitiveWordFile) {
		this.sensitiveWordFile = sensitiveWordFile;
	}
	
	public void setCONTENT_FIELD(String cONTENT_FIELD) {
		CONTENT_FIELD = cONTENT_FIELD;
	}
	
	public void setTITLE_FIELD(String tITLE_FIELD) {
		TITLE_FIELD = tITLE_FIELD;
	}
	
	public void setMAX_FIELD_LENGTH(int mAX_FIELD_LENGTH) {
		MAX_FIELD_LENGTH = mAX_FIELD_LENGTH;
	}

	@Override
	public ObjectNode process(ObjectNode result) throws Exception {
		ObjectNode source = result.with(ESConstants.R_HIT_SOURCE);

		if (source.has(CONTENT_FIELD)) {
			shorten(CONTENT_FIELD, source);
		}

		if (source.has(TITLE_FIELD)) {
			shorten(TITLE_FIELD, source);
		}

		return result;
	}

	private void sensitive_words(String field, ObjectNode source) {
		try {
			if(sensitivewordFilter==null) {
				sensitivewordFilter = new SensitivewordFilter(sensitiveWordFile);
			}
			String value = source.get(field).asText();
			source.remove(field);

			Set<String> sens = sensitivewordFilter.getSensitiveWord(value, 1);
			
			if(sens.size()==0) {
				source.put(field, value);
			}
			else {
				fLogger.info("find sensitive words:");
				fLogger.info(sens.toString());
			}
		} catch (Exception e) {
			// TODO: logging
			e.printStackTrace();
		}
	}
	
	private void shorten(String field, ObjectNode source) {
		try {
			String value = source.get(field).asText();
			source.remove(field);

			// only shorten if String is too long
			if (value.length() > MAX_FIELD_LENGTH) {
				value = value.substring(0, MAX_FIELD_LENGTH);
			}

			// remove any line breaks and carriage returns
			value = value.replace("\n", " ");
			value = value.replace("\r", " ");

			source.put(field, value);
		} catch (Exception e) {
			// TODO: logging
			e.printStackTrace();
		}
	}
}
