/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_PROJECTION_LSNR_HPP_INCLUDE
#define GRID_CLIENT_PROJECTION_LSNR_HPP_INCLUDE

#include "../gridclientnode.hpp"

/**
 * Projection listener.
 */
class GridClientProjectionListener {
public:
	/**
	 *
	 */
	virtual ~GridClientProjectionListener() { /* No-op. */ }

	/**
	 * @param n Node communication failed with.
	 */
	virtual void onNodeIoFailed(const GridClientNode& n) = 0;
};

#endif
