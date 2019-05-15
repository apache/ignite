package org.elasticsearch.relay.util;

public class ESConstants {
	// Query (URL) components
	public static final String SEARCH_FRAGMENT = "_search";
	public static final String UPDATE_FRAGMENT = "_update";
	public static final String DELETE_FRAGMENT = "_delete";
	public static final String INSERT_FRAGMENT = "_put";
	public static final String BULK_FRAGMENT = "_bulk";

	public static final String ALL_FRAGMENT = "_all";
	public static final String WILDCARD = "*";

	public static final String QUERY_PARAM = "q";

	// Query components (both URL and body)
	public static final String FIELDS_PARAM = "fields";
	public static final String SORT_PARAM = "sort";

	public static final String FIRST_ELEM_PARAM = "from";
	public static final String MAX_ELEM_PARAM = "size";

	public static final String SOURCE_PARAM = "_source";

	// Query body components
	public static final String Q_QUERY = "query";

	public static final String Q_FILTER = "filter";
	public static final String Q_FILTERED = "filtered";

	public static final String Q_NOT = "not";
	public static final String Q_AND = "and";
	public static final String Q_OR = "or";

	public static final String Q_TYPE = "type";
	public static final String Q_TERM = "term";
	public static final String Q_REGEX = "regexp";
	public static final String Q_VALUE = "value";

	public static final String Q_SORT_ORDER = "order";

	public static final String Q_SORT_ASCENDING = "asc";
	public static final String Q_SORT_DESCENDING = "desc";

	public static final String Q_SORT_SCRIPT = "_script";

	public static final String Q_NESTED_FILTER = "nested";

	// TODO: further sort options needed?
	// TODO: nested object sort options needed?
	// TODO: missing value sort options needed?
	// TODO: aggregation? would need to be processed

	// Response components
	public static final String R_ERROR = "error";
	public static final String R_STATUS = "status";

	public static final String R_SHARDS = "_shards";
	public static final String R_SHARDS_TOT = "total";
	public static final String R_SHARDS_SUC = "sucessful";
	public static final String R_SHARDS_FAIL = "failed";

	public static final String R_HITS = "hits";

	public static final String R_HITS_TOTAL = "total";

	public static final String R_HIT_INDEX = "_index";

	public static final String R_HIT_TYPE = "_type";

	public static final String R_HIT_ID = "_id";

	public static final String R_HIT_SOURCE = "_source";
}
