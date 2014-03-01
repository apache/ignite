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
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridClientMessageTaskResult : public GridClientMessageResult {
private:
    /** Task result. */
    GridClientVariant taskRslt_;
public:
    /**
     * Task result getter.
     *
     * @return Task result.
     */
    GridClientVariant getTaskResult() const {
        return taskRslt_;
    }

    /**
     * Task result setter.
     *
     * @param taskRslt Task result.
     */
    void setTaskResult(GridClientVariant taskRslt) {
        taskRslt_ = taskRslt;
    }
};

#endif
