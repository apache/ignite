/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_MESSAGE_TASK_RESULT_HPP_INCLUDED
#define GRID_CLIENT_MESSAGE_TASK_RESULT_HPP_INCLUDED

#include "gridgain/impl/cmd/gridclientmessageresult.hpp"

/**
 * Message task result.
 */
class GridClientMessageTaskResult : public GridClientMessageResult {
public:
    /** Task result. */
    GridClientVariant taskRes;
};

#endif
