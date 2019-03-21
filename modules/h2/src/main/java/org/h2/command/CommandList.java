/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command;

import java.util.ArrayList;

import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.expression.ParameterInterface;
import org.h2.result.ResultInterface;

/**
 * Represents a list of SQL statements.
 */
class CommandList extends Command {

    private CommandContainer command;
    private final ArrayList<Prepared> commands;
    private final ArrayList<Parameter> parameters;
    private String remaining;
    private Command remainingCommand;

    CommandList(Session session, String sql, CommandContainer command, ArrayList<Prepared> commands,
            ArrayList<Parameter> parameters, String remaining) {
        super(session, sql);
        this.command = command;
        this.commands = commands;
        this.parameters = parameters;
        this.remaining = remaining;
    }

    @Override
    public ArrayList<? extends ParameterInterface> getParameters() {
        return parameters;
    }

    private void executeRemaining() {
        for (Prepared prepared : commands) {
            prepared.prepare();
            if (prepared.isQuery()) {
                prepared.query(0);
            } else {
                prepared.update();
            }
        }
        if (remaining != null) {
            remainingCommand = session.prepareLocal(remaining);
            remaining = null;
            if (remainingCommand.isQuery()) {
                remainingCommand.query(0);
            } else {
                remainingCommand.update();
            }
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
    public void stop() {
        command.stop();
        for (Prepared prepared : commands) {
            CommandContainer.clearCTE(session, prepared);
        }
        if (remainingCommand != null) {
            remainingCommand.stop();
        }
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
