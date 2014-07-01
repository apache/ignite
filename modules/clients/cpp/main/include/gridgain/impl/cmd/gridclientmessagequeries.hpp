/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLEINT_MESSAGE_QUERIES_HPP_INCLUDED
#define GRID_CLEINT_MESSAGE_QUERIES_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientmessagecommand.hpp"
#include "gridgain/impl/cmd/gridclientmessageresult.hpp"

/**
 * Query request.
 */
class GridQueryRequestCommand : public GridClientMessageCommand {
public:

};

class GridClientDataQueryResult;

/**
 * Query execution result.
 */
class GridClientQueryResult : public GridClientMessageResult {
public:
    GridClientQueryResult() : res(0) {
    }

    GridClientDataQueryResult* res;
};

#endif
