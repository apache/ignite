package org.elasticsearch.relay.postprocess;

import java.util.Set;

import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.util.ESConstants;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;

/**
 * Mail result post processor adding user IDs to the entries "to" and "from",
 * i.e. for recipients and senders.
 */
public class MailPostProcessor implements IPostProcessor {
	private final String FROM = "from";
	private final String TO = "to";
	private final String EMAIL = "email";
	private final String ID = "uid";

	
	
	
	private Set<String>  typeSet= null;
	
	public Set<String> getTypeSet() {
		return typeSet;
	}

	public void setTypeSet(Set<String> typeSet) {
		this.typeSet = typeSet;
	}	

	@Override
	public JSONObject process(JSONObject result) throws Exception {
		JSONObject source = result.getJSONObject(ESConstants.R_HIT_SOURCE);

		// retrieve sender Id
		JSONObject fromObj = source.getJSONObject(FROM);
		if (fromObj != null) {
			String mail = fromObj.getString(EMAIL);
			String user = PermissionCrawler.getInstance().getUserByMail(mail);

			if (user != null) {
				fromObj.put(ID, user);
			}
		}

		// retrieve recipient IDs
		JSONArray toArray = source.getJSONArray(TO);
		if (toArray != null) {
			for (int i = 0; i < toArray.size(); ++i) {
				JSONObject recipient = toArray.getJSONObject(i);

				String mail = recipient.getString(EMAIL);
				String user = PermissionCrawler.getInstance().getUserByMail(mail);

				if (user != null) {
					recipient.put(ID, user);
				}
			}
		}

		return result;
	}
}
