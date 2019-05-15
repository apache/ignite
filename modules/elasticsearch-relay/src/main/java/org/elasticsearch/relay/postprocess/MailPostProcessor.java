package org.elasticsearch.relay.postprocess;

import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.util.ESConstants;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Mail result post processor adding user IDs to the entries "to" and "from",
 * i.e. for recipients and senders.
 */
public class MailPostProcessor implements IPostProcessor {
	private static final String FROM = "from";
	private static final String TO = "to";
	private static final String EMAIL = "email";
	private static final String ID = "id";

	private final PermissionCrawler fPerms;

	/**
	 * Creates a mail result post processor using the given permission crawler
	 * to look up user IDs using mail addresses.
	 * 
	 * @param perms
	 *            permission crawler to use
	 */
	public MailPostProcessor(PermissionCrawler perms) {
		fPerms = perms;
	}

	@Override
	public JSONObject process(JSONObject result) throws Exception {
		JSONObject source = result.getJSONObject(ESConstants.R_HIT_SOURCE);

		// retrieve sender Id
		JSONObject fromObj = source.optJSONObject(FROM);
		if (fromObj != null) {
			String mail = fromObj.optString(EMAIL);
			String user = fPerms.getUserByMail(mail);

			if (user != null) {
				fromObj.put(ID, user);
			}
		}

		// retrieve recipient IDs
		JSONArray toArray = source.optJSONArray(TO);
		if (toArray != null) {
			for (int i = 0; i < toArray.length(); ++i) {
				JSONObject recipient = toArray.getJSONObject(i);

				String mail = recipient.optString(EMAIL);
				String user = fPerms.getUserByMail(mail);

				if (user != null) {
					recipient.put(ID, user);
				}
			}
		}

		return result;
	}
}
