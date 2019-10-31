/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;

/**
 * Represents a list of SQL statements.
 */
class CommandList extends Command {

    private final Command command;
    private final String remaining;

    CommandList(Parser parser, String sql, Command c, String remaining) {
        super(parser, sql);
        this.command = c;
        this.remaining = remaining;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return command.getParameters();
    }

    private void executeRemaining() {
        Command remainingCommand = session.prepareLocal(remaining);
        if (remainingCommand.isQuery()) {
            remainingCommand.query(0);
        } else {
            remainingCommand.update();
        }
    }

    @Override
    public int update() {
        int updateCount = command.executeUpdate(false).getUpdateCount();
        executeRemaining();
        return updateCount;
    }

    @Override
    public void prepareJoinBatch() {
        command.prepareJoinBatch();
    }

    @Override
    public ResultInterface query(int maxrows) {
        ResultInterface result = command.query(maxrows);
        executeRemaining();
        return result;
    }

    @Override
    public boolean isQuery() {
        return command.isQuery();
    }

    @Override
    public boolean isTransactional() {
        return true;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public ResultInterface queryMeta() {
        return command.queryMeta();
    }

    @Override
    public int getCommandType() {
        return command.getCommandType();
    }

}
