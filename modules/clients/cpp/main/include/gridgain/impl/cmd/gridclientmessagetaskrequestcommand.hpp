/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLIENT_MESSAGE_TASK_REQUEST_COMMAND_HPP_INCLUDED
#define GRID_CLIENT_MESSAGE_TASK_REQUEST_COMMAND_HPP_INCLUDED

#include <string>

#include "gridgain/gridclienttypedef.hpp"
#include "gridgain/impl/cmd/gridclientmessagecommand.hpp"

/**
 * Generic message result.
 *
 * @author @cpp.author
 * @version @cpp.version
 */
class GridTaskRequestCommand : public GridClientMessageCommand {
public:
    /**
     * Method for retrieving task arguments.
     *
     * @return Task argument.
     */
    GridClientVariant getArg() const {
        return arg;
    }

    /**
     * Method for retrieving task name.
     *
     * @return Task name.
     */
    std::string getTaskName() const {
        return taskName;
    }

    /**
     * Set new task argument.
     *
     * @param pArg Task argument.
     */
    void setArg(const GridClientVariant& pArg) {
        arg = pArg;
    }

    /**
     * Set new task name.
     *
     * @param pTaskname Task name.
     */
    void setTaskName(const std::string& pTaskName) {
        taskName = pTaskName;
    }

private:
    /** Task name. */
    std::string taskName;

    /** Task argument. */
    GridClientVariant arg;
};

#endif
