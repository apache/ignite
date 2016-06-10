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
 * Sql statements used to manage the discovery database
 */
public interface DiscoveryJdbcDialect {
	/**
	 * The sql query to load all addresses
	 * @return the query
	 */

	String loadAddressesQuery();

	/**
	 * The sql query to unregister an addresses
	 * @return the query
	 */
	String unregisterAddressQuery();

	/**
	 * The sql query to register an addresses
	 * @return the query
	 */
	String registerAddressQuery();

	/**
	 * The sql query to create the table
	 * @return the query
	 */
	String createTableQuery();

	/**
	 * The table name, quoted for metadata lookup
	 * @return the table name
	 */
	String getTableNameForMetadata();

	/**
	 * The sql query the check that the table exists
	 * @return the query
	 */
	String checkTableExistsQuery();
}
