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

	private static final String ES_CLUSTER = "elasticsearch.cluster.name";
	private static final String ES_URL = "elasticsearch.url";
	private static final String ES_API_HOST = "elasticsearch.api.host";
	private static final String ES_API_PORT = "elasticsearch.api.port";

	private static final String ES_INDICES = "elasticsearch.indices";

	private static final String ES_BL_INDICES = "elasticsearch.indices.blacklist";
	private static final String ES_BL_TYPES = "elasticsearch.types.blacklist";

	private static final String ES2_CLUSTER = "elasticsearch2.cluster.name";
	private static final String ES2_URL = "elasticsearch2.url";
	private static final String ES2_API_HOST = "elasticsearch2.api.host";
	private static final String ES2_API_PORT = "elasticsearch2.api.port";

	private static final String ES2_INDICES = "elasticsearch2.indices";

	private static final String ES2_BL_INDICES = "elasticsearch2.indices.blacklist";
	private static final String ES2_BL_TYPES = "elasticsearch2.types.blacklist";

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

	public String getClusterName() {
		return fProperties.get(ES_CLUSTER);
	}

	public String getElasticUrl() {
		return fProperties.get(ES_URL);
	}

	public String getElasticApiHost() {
		return fProperties.get(ES_API_HOST);
	}

	public int getElasticApiPort() {
		return Integer.parseInt(fProperties.get(ES_API_PORT));
	}

	public Set<String> getElasticIndices() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(ES_INDICES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public Set<String> getEs1BlacklistIndices() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(ES_BL_INDICES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public Set<String> getEs1BlacklistTypes() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(ES_BL_TYPES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public String getEs2ClusterName() {
		return fProperties.get(ES2_CLUSTER);
	}

	public String getEs2ElasticUrl() {
		return fProperties.get(ES2_URL);
	}

	public String getEs2ElasticApiHost() {
		return fProperties.get(ES2_API_HOST);
	}

	public int getEs2ElasticApiPort() {
		return Integer.parseInt(fProperties.get(ES2_API_PORT));
	}

	public Set<String> getEs2Indices() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(ES2_INDICES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public Set<String> getEs2BlacklistIndices() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(ES2_BL_INDICES).split(",")) {
			indices.add(index);
		}

		return indices;
	}

	public Set<String> getEs2BlacklistTypes() {
		Set<String> indices = new HashSet<String>();

		for (String index : fProperties.get(ES2_BL_TYPES).split(",")) {
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
