package org.apache.ignite.internal.commandline;

/**
 * Bean with all parsed and validated arguments
 */
class Arguments {
    /** Command. */
    private String cmd;

    /** Host. */
    private String host;

    /** Port. */
    private String port;

    /** User. */
    private String user;

    /** Password. */
    private String pwd;

    /**
     * Action for baseline command
     */
    private String baselineAct;

    /**
     * Arguments for baseline command
     */
    private String baselineArgs;

    /**
     * @param cmd Command.
     * @param host Host.
     * @param port Port.
     * @param user User.
     * @param pwd Password.
     * @param baselineAct Baseline action.
     * @param baselineArgs Baseline args.
     */
    public Arguments(String cmd, String host, String port, String user, String pwd, String baselineAct,
        String baselineArgs) {
        this.cmd = cmd;
        this.host = host;
        this.port = port;
        this.user = user;
        this.pwd = pwd;
        this.baselineAct = baselineAct;
        this.baselineArgs = baselineArgs;
    }

    /**
     * @return command
     */
    public String command() {
        return cmd;
    }

    /**
     * @return host name
     */
    public String host() {
        return host;
    }

    /**
     * @return port number
     */
    public String port() {
        return port;
    }

    /**
     * @return user name
     */
    public String user() {
        return user;
    }

    /**
     * @return password
     */
    public String password() {
        return pwd;
    }

    /**
     * @return baseline action
     */
    public String baselineAction() {
        return baselineAct;
    }

    /**
     * @return baseline arguments
     */
    public String baselineArguments() {
        return baselineArgs;
    }
}
