// @cpp.file.header

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRIDCLIENTAPIEXAMPLE_HPP_
#define GRIDCLIENTAPIEXAMPLE_HPP_

#include <string>

#include <gridgain/gridgain.hpp>

/** Remote nodes host name. */
const std::string SERVER_ADDRESS = "127.0.0.1";

/** Remote nodes starting TCP port. */
const int TCP_PORT = 11211;

/** Max nodes to connect to. */
const int MAX_NODES = 5;

/** Name of the cache to work with. */
const std::string CACHE_NAME = "partitioned";

/** Number of keys to use in corresponding examples. */
const int KEYS_CNT = 10;

/**
 * This method configures the client for an example.
 * Modify it to change connection parameters and other
 * options.
 *
 * @return A configuration for GridGain client.
 */
GridClientConfiguration clientConfiguration();

#endif /* GRIDCLIENTAPIEXAMPLE_HPP_ */
