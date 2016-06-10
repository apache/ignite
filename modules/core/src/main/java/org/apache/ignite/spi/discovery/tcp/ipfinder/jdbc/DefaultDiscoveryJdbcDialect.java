/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ignite.spi.discovery.tcp.ipfinder.jdbc;

/**
 * Default implementation of DiscoveryJdbcDialect with support for common quoting rules.
 */

public class DefaultDiscoveryJdbcDialect implements DiscoveryJdbcDialect {
	private final String metadataTableName;

	/**
	 * Query to get addresses.
	 */
	private final String getAddrsQry;

	/**
	 * Query to register address.
	 */
	private final String regAddrQry;

	/**
	 * Query to unregister address.
	 */
	private final String unregAddrQry;

	/**
	 * Query to create addresses table.
	 */
	private final String createAddrsTableQry;

	/**
	 * Query to check database validity.
	 */
	private final String chkQry;

	private String quotedTableName(String quoteSequence, String table) {
		return quoteSequence != null ? quoteSequence + table + quoteSequence : table;
	}

	public DefaultDiscoveryJdbcDialect(String tableName, String metadataTableName, String quoteSequence) {
		String nameToUse = quotedTableName(quoteSequence, tableName);
		this.metadataTableName = quotedTableName( quoteSequence, metadataTableName);
		getAddrsQry = "select hostname, port from " + nameToUse;
		regAddrQry = "insert into " + nameToUse + " values (?, ?)";
		unregAddrQry = "delete from " + nameToUse + " where hostname = ? and port = ?";
		createAddrsTableQry = "create table " + nameToUse + " (hostname VARCHAR(1024), port INT)";
		chkQry = "select count(*) from " + nameToUse;
	}

	/** {@inheritDoc} */
	@Override public String loadAddressesQuery() {
		return getAddrsQry;
	}

	/** {@inheritDoc} */
	@Override public String unregisterAddressQuery() {
		return unregAddrQry;
	}

	/** {@inheritDoc} */
	@Override public String registerAddressQuery() {
		return regAddrQry;
	}

	/** {@inheritDoc} */
	@Override public String createTableQuery() {
		return createAddrsTableQry;
	}

	/** {@inheritDoc} */
	@Override public String getTableNameForMetadata() {
		return metadataTableName;
	}

	/** {@inheritDoc} */
	@Override public String checkTableExistsQuery() {
		return chkQry;
	}


	/**
	 * A generic dialect that does not use quoting
	 * @return the generic dialect
	 */
	public static DiscoveryJdbcDialect generic() {
		return new DefaultDiscoveryJdbcDialect("tbl_addrs", "tbl_addrs", null);
	}

	/**
	 * A dialect that works well with mysql
	 * @return the mysql dialect
	 */
	public static DiscoveryJdbcDialect mysql() {
		// case sensitive if underlying file system is case sensitive
		return new DefaultDiscoveryJdbcDialect("tbl_addrs", "tbl_addrs", null);
	}

	/**
	 * A dialect that works well with oracle
	 * @return the oracledialect
	 */
	public static DiscoveryJdbcDialect oracle() {
		return new DefaultDiscoveryJdbcDialect("TBL_ADDRS", "TBL_ADDRS", "\"");
	}
}
