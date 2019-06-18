package org.elasticsearch.relay.postprocess;

import java.util.Set;
import java.util.logging.Logger;

import org.elasticsearch.relay.util.ESConstants;
import org.elasticsearch.relay.util.SensitivewordFilter;
import com.alibaba.fastjson.JSONObject;

/** 过滤敏感词
 *  如果是标题，过滤回车符，截断过长的标题
 * @author WBPC1158
 *
 */
public class ContentPostProcessor implements IPostProcessor {
	private static final String CONTENT_FIELD = "content";
	private static final String TITLE_FIELD = "title";

	private static final int MAX_FIELD_LENGTH = 255;
	
	
	static Logger fLogger = Logger.getLogger(ContentPostProcessor.class.getName());
	
	SensitivewordFilter sensitivewordFilter;
	
	
	private String sensitiveWordFile = "sensitive_words.txt";

	public String getSensitiveWordFile() {
		return sensitiveWordFile;
	}

	public void setSensitiveWordFile(String sensitiveWordFile) {
		this.sensitiveWordFile = sensitiveWordFile;
	}

	@Override
	public JSONObject process(JSONObject result) throws Exception {
		JSONObject source = result.getJSONObject(ESConstants.R_HIT_SOURCE);

		if (source.containsKey(CONTENT_FIELD)) {
			shorten(CONTENT_FIELD, source);
		}

		if (source.containsKey(TITLE_FIELD)) {
			shorten(TITLE_FIELD, source);
		}

		return result;
	}

	private void sensitive_words(String field, JSONObject source) {
		try {
			if(sensitivewordFilter==null) {
				sensitivewordFilter = new SensitivewordFilter(sensitiveWordFile);
			}
			String value = source.getString(field);
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
	
	private void shorten(String field, JSONObject source) {
		try {
			String value = source.getString(field);
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
