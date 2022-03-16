package org.elasticsearch.relay.permissions;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.elasticsearch.relay.ESRelay;
import org.elasticsearch.relay.util.HttpUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Main permission crawler triggering all other crawlers regularly and
 * collecting the results. Collects all users and also offers a user ID lookup
 * by Liferay ID and mail address.
 */
public class PermissionCrawler implements IPermCrawler, Runnable {
	//获取所有用户 "social/rest/user?fields=id,emails&count=0"
	private static final String SHIND_ALL_USERS_QUERY = "?cmd=qryfldexe&cacheName=Person&pageSize=100000&qry=select+id%2C+email+from+Person";
	
		
	private static final String SHIND_LIST_FIELD = "list";
	private static final String SHIND_ID_FIELD = "id";
	private static final String SHIND_EMAILS_FIELD = "emails";
	private static final String SHIND_VALUE_FIELD = "value";
	
	
	private static PermissionCrawler fPermCrawler = null;
	
	

	private final Object fTrigger = new Object();

	private final long fInterval;

	private final URL fShindigUrl;

	private final Map<String, UserPermSet> fPermissions;

	private final Map<String, String> fUsersByLrId;

	private final Map<String, String> fUsersByMail;

	private final List<IPermCrawler> fCrawlers;

	private final List<String> fUsers;

	private final Logger fLogger;

	private boolean fActive;
	
	public static PermissionCrawler getInstance() {
		return fPermCrawler;
	}

	/**
	 * Creates a runnable crawler using the list of people retrieved from the
	 * given Shindig URL to retrieve permissions using the given crawlers. Does
	 * not start a thread on its own.
	 * 
	 * @param shindigUrl
	 *            URL of shindig for user list retrieval
	 * @param crawlers
	 *            list of sub-crawlers to use
	 * @param interval
	 *            interval in millseconds between crawls
	 * @throws Exception
	 *             if intialization fails
	 */
	public PermissionCrawler(String shindigUrl, List<IPermCrawler> crawlers, long interval) throws Exception {
		fInterval = interval;

		fShindigUrl = new URL(shindigUrl + SHIND_ALL_USERS_QUERY);

		fPermissions = new HashMap<String, UserPermSet>();

		fUsersByLrId = new HashMap<String, String>();

		fUsersByMail = new HashMap<String, String>();

		fCrawlers = crawlers;

		fUsers = new ArrayList<String>();

		fLogger = Logger.getLogger(this.getClass().getName());
		
		fPermCrawler = this;
	}

	@Override
	public UserPermSet getPermissions(String name) {
		return fPermissions.get(name);
	}

	@Override
	public UserPermSet getPermissions(UserPermSet perms) {
		return fPermissions.get(perms.getUserName());
	}

	public String getUserByMail(String mail) {
		return fUsersByMail.get(mail);
	}

	public String getUserByLiferayId(String id) {
		return fUsersByLrId.get(id);
	}

	@Override
	public void run() {
		fActive = true;

		if (fActive) {			
			
			if(fInterval<=0) { 
				fActive = false;
			}			
			
			try {
				fLogger.log(Level.INFO, "starting permission crawl");
				long time = System.currentTimeMillis();

				crawl();

				time = System.currentTimeMillis() - time;
				fLogger.log(Level.INFO, "finished crawl in " + time + "ms");
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}

	/**
	 * Triggers one crawl iteration, refreshing the stored permission sets using
	 * all sub-crawlers.
	 * 
	 * @throws Exception
	 *             if crawling fails
	 */
	public void crawl() throws Exception {
		// clear users
		fUsers.clear();

		// create temporary permission map before the long crawl
		Map<String, UserPermSet> tempPerms = new HashMap<String, UserPermSet>();

		Map<String, String> tempMails = new HashMap<String, String>();
		Map<String, String> tempLrIds = new HashMap<String, String>();

		// retrieve list of users from configured source (shindig)
		String response = HttpUtil.getText(fShindigUrl);

		ObjectNode resObj = (ObjectNode)ESRelay.objectMapper.readTree(response);
		
		ArrayNode list = resObj.withArray(SHIND_LIST_FIELD);
		if (list != null) {
			JsonNode entry = null;
			for (int i = 0; i < list.size(); ++i) {
				entry = list.get(i);

				// register user
				String userId = entry.get(SHIND_ID_FIELD).asText();
				fUsers.add(userId);

				// register all mail addresses
				ArrayNode mailAdds = entry.withArray(SHIND_EMAILS_FIELD);
				for (int j = 0; j < mailAdds.size(); ++j) {
					String mail = mailAdds.get(j).get(SHIND_VALUE_FIELD).asText();
					tempMails.put(mail, userId);
				}
			}
		}

		// crawl all permissions for all users
		for (String user : fUsers) {
			UserPermSet set = new UserPermSet(user);

			for (IPermCrawler crawler : fCrawlers) {
				set = crawler.getPermissions(set);
			}

			// register set of permissions
			tempPerms.put(user, set);

			// register liferay ID
			tempLrIds.put(set.getLiferayId(), user);
		}

		// replace old permissions and lookups with new ones
		fPermissions.clear();
		fPermissions.putAll(tempPerms);

		fUsersByLrId.clear();
		fUsersByLrId.putAll(tempLrIds);

		fUsersByMail.clear();
		fUsersByMail.putAll(tempMails);
	}

	/**
	 * Stops any crawler threads.
	 */
	public void stop() {
		fActive = false;

		synchronized (fTrigger) {
			fTrigger.notify();
		}
	}
}
