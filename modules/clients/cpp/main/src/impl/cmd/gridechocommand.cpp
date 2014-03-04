/* @cpp.file.header */

/*  _________        _____ __________________        _____
 *  __  ____/___________(_)______  /__  ____/______ ____(_)_______
 *  _  / __  __  ___/__  / _  __  / _  / __  _  __ `/__  / __  __ \
 *  / /_/ /  _  /    _  /  / /_/ /  / /_/ /  / /_/ / _  /  _  / / /
 *  \____/   /_/     /_/   \_,__/   \____/   \__,_/  /_/   /_/ /_/
 */
#include "gridgain/impl/utils/gridclientdebug.hpp"

#include "gridgain/impl/cmd/gridclientmessageechocommand.hpp"

/**
 * Generates bytes to be sent as an echo command.
 *
 * @param bytes Vector to fill.
 */
void GridClientMessageEchoCommand::convertToBytes(std::vector<int8_t>& bytes) const {
    bytes.push_back((char)0x90);
    bytes.push_back(0);
    bytes.push_back(0);
    bytes.push_back(0);
}

