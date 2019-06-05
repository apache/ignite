/*
 * Copyright 2004-2018 H2 Group. Multiple-Licensed under the MPL 2.0,
 * and the EPL 1.0 (http://h2database.com/html/license.html).
 * Initial Developer: H2 Group
 */
package org.h2.command.ddl;

import org.h2.api.ErrorCode;
import org.h2.command.CommandInterface;
import org.h2.engine.Database;
import org.h2.engine.Session;
import org.h2.engine.User;
import org.h2.expression.Expression;
import org.h2.message.DbException;
import org.h2.security.SHA256;
import org.h2.util.StringUtils;

/**
 * This class represents the statement
 * CREATE USER
 */
public class CreateUser extends DefineCommand {

    private String userName;
    private boolean admin;
    private Expression password;
    private Expression salt;
    private Expression hash;
    private boolean ifNotExists;
    private String comment;

    public CreateUser(Session session) {
        super(session);
    }

    public void setIfNotExists(boolean ifNotExists) {
        this.ifNotExists = ifNotExists;
    }

    public void setUserName(String userName) {
        this.userName = userName;
    }

    public void setPassword(Expression password) {
        this.password = password;
    }

    /**
     * Set the salt and hash for the given user.
     *
     * @param user the user
     * @param session the session
     * @param salt the salt
     * @param hash the hash
     */
    static void setSaltAndHash(User user, Session session, Expression salt, Expression hash) {
        user.setSaltAndHash(getByteArray(session, salt), getByteArray(session, hash));
    }

    private static byte[] getByteArray(Session session, Expression e) {
        String s = e.optimize(session).getValue(session).getString();
        return s == null ? new byte[0] : StringUtils.convertHexToBytes(s);
    }

    /**
     * Set the password for the given user.
     *
     * @param user the user
     * @param session the session
     * @param password the password
     */
    static void setPassword(User user, Session session, Expression password) {
        String pwd = password.optimize(session).getValue(session).getString();
        char[] passwordChars = pwd == null ? new char[0] : pwd.toCharArray();
        byte[] userPasswordHash;
        String userName = user.getName();
        if (userName.length() == 0 && passwordChars.length == 0) {
            userPasswordHash = new byte[0];
        } else {
            userPasswordHash = SHA256.getKeyPasswordHash(userName, passwordChars);
        }
        user.setUserPasswordHash(userPasswordHash);
    }

    @Override
    public int update() {
        session.getUser().checkAdmin();
        session.commit(true);
        Database db = session.getDatabase();
        if (db.findRole(userName) != null) {
            throw DbException.get(ErrorCode.ROLE_ALREADY_EXISTS_1, userName);
        }
        if (db.findUser(userName) != null) {
            if (ifNotExists) {
                return 0;
            }
            throw DbException.get(ErrorCode.USER_ALREADY_EXISTS_1, userName);
        }
        int id = getObjectId();
        User user = new User(db, id, userName, false);
        user.setAdmin(admin);
        user.setComment(comment);
        if (hash != null && salt != null) {
            setSaltAndHash(user, session, salt, hash);
        } else if (password != null) {
            setPassword(user, session, password);
        } else {
            throw DbException.throwInternalError();
        }
        db.addDatabaseObject(session, user);
        return 0;
    }

    public void setSalt(Expression e) {
        salt = e;
    }

    public void setHash(Expression e) {
        hash = e;
    }

    public void setAdmin(boolean b) {
        admin = b;
    }

    public void setComment(String comment) {
        this.comment = comment;
    }

    @Override
    public int getType() {
        return CommandInterface.CREATE_USER;
    }

}
