package org.elasticsearch.relay.postprocess;

import java.util.Set;

import org.elasticsearch.relay.permissions.PermissionCrawler;
import org.elasticsearch.relay.util.ESConstants;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


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
	public ObjectNode process(ObjectNode result) throws Exception {
		ObjectNode source = result.with(ESConstants.R_HIT_SOURCE);

		// retrieve sender Id
		ObjectNode fromObj = source.with(FROM);
		if (fromObj != null) {
			String mail = fromObj.get(EMAIL).asText();
			String user = PermissionCrawler.getInstance().getUserByMail(mail);

			if (user != null) {
				fromObj.put(ID, user);
			}
		}

		// retrieve recipient IDs
		ArrayNode toArray = source.withArray(TO);
		if (toArray != null) {
			for (int i = 0; i < toArray.size(); ++i) {
				ObjectNode recipient = (ObjectNode)toArray.get(i);

				String mail = recipient.get(EMAIL).asText();
				String user = PermissionCrawler.getInstance().getUserByMail(mail);

				if (user != null) {
					recipient.put(ID, user);
				}
			}
		}

		return result;
	}
}
