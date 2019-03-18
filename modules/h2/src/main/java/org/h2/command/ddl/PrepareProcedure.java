/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import java.util.ArrayList;
import org.h2.command.CommandInterface;
import org.h2.command.Prepared;
import org.h2.engine.Procedure;
import org.h2.engine.Session;
import org.h2.expression.Parameter;
import org.h2.util.New;

/**
 * This class represents the statement
 * PREPARE
 */
public class PrepareProcedure extends DefineCommand {

    private String procedureName;
    private Prepared prepared;

    public PrepareProcedure(Session session) {
        super(session);
    }

    @Override
    public void checkParameters() {
        // no not check parameters
    }

    @Override
    public int update() {
        Procedure proc = new Procedure(procedureName, prepared);
        prepared.setParameterList(parameters);
        prepared.setPrepareAlways(prepareAlways);
        prepared.prepare();
        session.addProcedure(proc);
        return 0;
    }

    public void setProcedureName(String name) {
        this.procedureName = name;
    }

    public void setPrepared(Prepared prep) {
        this.prepared = prep;
    }

    @Override
    public ArrayList<Parameter> getParameters() {
        return New.arrayList();
    }

    @Override
    public int getType() {
        return CommandInterface.PREPARE;
    }

}
