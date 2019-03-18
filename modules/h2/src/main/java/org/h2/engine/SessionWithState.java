/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.engine;

import java.util.ArrayList;
import org.h2.command.CommandInterface;
import org.h2.result.ResultInterface;
import org.h2.util.New;
import org.h2.value.Value;

/**
 * The base class for both remote and embedded sessions.
 */
abstract class SessionWithState implements SessionInterface {

    protected ArrayList<String> sessionState;
    protected boolean sessionStateChanged;
    private boolean sessionStateUpdating;

    /**
     * Re-create the session state using the stored sessionState list.
     */
    protected void recreateSessionState() {
        if (sessionState != null && !sessionState.isEmpty()) {
            sessionStateUpdating = true;
            try {
                for (String sql : sessionState) {
                    CommandInterface ci = prepareCommand(sql, Integer.MAX_VALUE);
                    ci.executeUpdate(false);
                }
            } finally {
                sessionStateUpdating = false;
                sessionStateChanged = false;
            }
        }
    }

    /**
     * Read the session state if necessary.
     */
    public void readSessionState() {
        if (!sessionStateChanged || sessionStateUpdating) {
            return;
        }
        sessionStateChanged = false;
        sessionState = New.arrayList();
        CommandInterface ci = prepareCommand(
                "SELECT * FROM INFORMATION_SCHEMA.SESSION_STATE",
                Integer.MAX_VALUE);
        ResultInterface result = ci.executeQuery(0, false);
        while (result.next()) {
            Value[] row = result.currentRow();
            sessionState.add(row[1].getString());
        }
    }

}
