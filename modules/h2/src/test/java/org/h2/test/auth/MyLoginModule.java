/*
 * Copyright 2004-2019 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: Alessandro Ventura
 */
package org.h2.test.auth;

import java.util.Map;
import javax.security.auth.Subject;
import javax.security.auth.callback.Callback;
import javax.security.auth.callback.CallbackHandler;
import javax.security.auth.callback.NameCallback;
import javax.security.auth.callback.PasswordCallback;
import javax.security.auth.login.LoginException;
import javax.security.auth.spi.LoginModule;

/**
 * Dummy login module used for test cases
 */
public class MyLoginModule implements LoginModule{

    String password;
    CallbackHandler callbackHandler;

    @Override
    public void initialize(Subject subject, CallbackHandler callbackHandler, Map<String, ?> sharedState,
            Map<String, ?> options) {
        this.callbackHandler=callbackHandler;
        password=""+options.get("password");
    }

    @Override
    public boolean login() throws LoginException {
        if (callbackHandler==null) {
            throw new LoginException("no callbackHandler available");
        }
        NameCallback nameCallback= new NameCallback("user name");
        PasswordCallback passwordCallback= new PasswordCallback("user name",false);
        try {
            callbackHandler.handle(new Callback[] {nameCallback,passwordCallback});
        } catch (Exception exception) {
            throw new LoginException("an exception occurred during inquiry of username and password");
        }
        return password.equals(new String(passwordCallback.getPassword()));
    }

    @Override
    public boolean logout() throws LoginException {
        return true;
    }

    @Override
    public boolean abort() throws LoginException {
        return true;
    }

    @Override
    public boolean commit() throws LoginException {
        return true;
    }
}
