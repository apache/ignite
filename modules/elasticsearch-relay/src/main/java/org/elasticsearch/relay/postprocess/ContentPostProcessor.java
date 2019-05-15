package org.elasticsearch.relay.postprocess;

import org.elasticsearch.relay.util.ESConstants;
import org.json.JSONObject;

public class ContentPostProcessor implements IPostProcessor {
	private static final String CONTENT_FIELD = "content";
	private static final String TEXT_CONTENT_FIELD = "textContent";

	private static final int MAX_FIELD_LENGTH = 255;

	@Override
	public JSONObject process(JSONObject result) throws Exception {
		JSONObject source = result.getJSONObject(ESConstants.R_HIT_SOURCE);

		if (source.has(CONTENT_FIELD)) {
			shorten(CONTENT_FIELD, source);
		}

		if (source.has(TEXT_CONTENT_FIELD)) {
			shorten(TEXT_CONTENT_FIELD, source);
		}

		return result;
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
