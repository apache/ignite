/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */

#ifndef GRID_CLEINTMESSAGE_ECHO_COMMAND_HPP_INCLUDED
#define GRID_CLEINTMESSAGE_ECHO_COMMAND_HPP_INCLUDED

#include <cstdint>
#include <vector>

#include "gridgain/impl/cmd/gridclientmessagecommand.hpp"

/**
 * Echo command.
 */
class GridClientMessageEchoCommand : public GridClientMessageCommand {
public:
    /**
     * Generates bytes to be sent as an echo command.
     *
     * @param bytes Vector to fill.
     */
    virtual void convertToBytes(std::vector<int8_t>& bytes) const;
};

#endif
