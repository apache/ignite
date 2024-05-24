package org.elasticsearch.relay;

/**
 * Elasticsearch Relay main configuration class reading all settings from
 * "elasticsearch-relay.properties" and offering them via its getters.
 */
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.ResourceBundle;
import java.util.Set;

public class ESRelayConfig {
	private static final String PROPERTIES = "elasticsearch-relay";
	
	private static final String ES_CLUSTER_BACKEND = "elasticsearch.relay.backend";

	private static final String PERM_CRAWL_INT = "elasticsearch.relay.permissions.crawl_interval";
	
	//Perm get url:
	private static final String PERM_CRAWL_URL = "elasticsearch.relay.permissions.crawl_url";

	private static final String LOG_REQUESTS = "elasticsearch.relay.log_requests";	

	// indexed config
	private static final String ES_CLUSTER = "elasticsearch%d.cluster.name";
	private static final String ES_URL = "elasticsearch%d.url";
	private static final String ES_API_HOST = "elasticsearch%d.api.host";
	private static final String ES_API_PORT = "elasticsearch%d.api.port";

	private static final String ES_INDICES = "elasticsearch%d.indices";

	private static final String ES_BL_INDICES = "elasticsearch%d.indices.blacklist";
	private static final String ES_BL_TYPES = "elasticsearch%d.types.blacklist";

	

	private static final String LIFERAY_INDEX = "elasticsearch.relay.liferay_index";
	private static final String LIFERAY_TYPES = "elasticsearch.relay.liferay_types";

	private static final String LIFERAY_URL = "elasticsearch.relay.liferay_url";
	private static final String LIFERAY_COMP_ID = "elasticsearch.relay.liferay_company_id";
	private static final String LIFERAY_USER = "elasticsearch.relay.liferay_user";
	private static final String LIFERAY_PASSWORD = "elasticsearch.relay.liferay_password";

	private static final String LIFERAY_PASS_ROLES = "elasticsearch.relay.liferay_passthrough_roles";

	private static final String NUXEO_INDEX = "elasticsearch.relay.nuxeo_index";
	private static final String NUXEO_TYPES = "elasticsearch.relay.nuxeo_types";

	private static final String NUXEO_URL = "elasticsearch.relay.nuxeo_url";
	private static final String NUXEO_USER = "elasticsearch.relay.nuxeo_user";
	private static final String NUXEO_PASSWORD = "elasticsearch.relay.nuxeo_password";

	private static final String MAIL_INDEX = "elasticsearch.relay.mail_index";
	private static final String MAIL_TYPES = "elasticsearch.relay.mail_type";

	private final Map<String, String> fProperties;

	public ESRelayConfig() {
		fProperties = new HashMap<String, String>();

		ClassLoader loader = Thread.currentThread().getContextClassLoader();
		ResourceBundle rb = ResourceBundle.getBundle(PROPERTIES, Locale.getDefault(), loader);

		String key = null;
		String value = null;
		Enumeration<String> keys = rb.getKeys();
		while (keys.hasMoreElements()) {
			key = keys.nextElement();
			value = rb.getString(key);

			fProperties.put(key, value);
		}
	}

	public Map<String, String> getProperties() {
		return fProperties;
	}

	public String getPermissionsCrawlUrl() {
		return fProperties.get(PERM_CRAWL_URL);
	}
	
	public String getClusterBackend(){
		return fProperties.get(ES_CLUSTER_BACKEND);
	}
	
	public long getPermCrawlInterval() {
		return Long.parseLong(fProperties.get(PERM_CRAWL_INT));
	}

	public boolean getLogRequests() {
		return Boolean.parseBoolean(fProperties.get(LOG_REQUESTS));
	}
	
	// begin with 1
	public int getClusterSize() {
		for(int i=1;i<Integer.MAX_VALUE;i++) {
			String name = this.getClusterName(i);
			if(name==null) {
				return i-1;
			}
		}
		return Integer.MAX_VALUE;
	}

	public String getClusterName(int i) {
		return fProperties.get(String.format(ES_CLUSTER,i));
	}

	public String getElasticUrl(int i) {
		return fProperties.get(String.format(ES_URL,i));
	}

	public String getElasticApiHost(int i) {
		return fProperties.get(String.format(ES_API_HOST,i));
	}

	public int getElasticApiPort(int i) {
		return Integer.parseInt(fProperties.get(String.format(ES_API_PORT,i)));
	}

	public Set<String> getElasticIndices(int i) {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(String.format(ES_INDICES,i)).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public Set<String> getEsBlacklistIndices(int i) {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(String.format(ES_BL_INDICES,i)).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public Set<String> getEsBlacklistTypes(int i) {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(String.format(ES_BL_TYPES,i)).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	

	public String getLiferayIndex() {
		return fProperties.get(LIFERAY_INDEX);
	}

	public Set<String> getLiferayTypes() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(LIFERAY_TYPES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public String getLiferayUrl() {
		return fProperties.get(LIFERAY_URL);
	}

	public String getLiferayCompanyId() {
		return fProperties.get(LIFERAY_COMP_ID);
	}

	public String getLiferayUser() {
		return fProperties.get(LIFERAY_USER);
	}

	public String getLiferayPassword() {
		return fProperties.get(LIFERAY_PASSWORD);
	}

	public Set<String> getLiferayPassthroughRoles() {
		Set<String> roles = new HashSet<String>();

		for (String index : fProperties.get(LIFERAY_PASS_ROLES).split(",")) {
			roles.add(index);
		}

		return roles;
	}

	public String getNuxeoIndex() {
		return fProperties.get(NUXEO_INDEX);
	}

	public Set<String> getNuxeoTypes() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(NUXEO_TYPES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public String getNuxeoUrl() {
		return fProperties.get(NUXEO_URL);
	}

	public String getNuxeoUser() {
		return fProperties.get(NUXEO_USER);
	}

	public String getNuxeoPassword() {
		return fProperties.get(NUXEO_PASSWORD);
	}

	public String getMailIndex() {
		return fProperties.get(MAIL_INDEX);
	}

	public Set<String> getMailTypes() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(MAIL_TYPES).split(",")) {
			indices.add(index);
		}

		return indices;
	}


}
