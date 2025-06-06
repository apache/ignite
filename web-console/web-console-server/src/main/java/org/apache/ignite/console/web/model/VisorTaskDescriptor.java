

package org.apache.ignite.console.web.model;

/**
 * Descriptor of Visor task.
 */
public class VisorTaskDescriptor {
    /** */
    private static final String[] EMPTY = new String[0];

    /** */
    private final String taskCls;

    /** */
    private final String[] argCls;

    /**
     * @param taskCls Visor task class.
     * @param argCls Visor task arguments classes.
     */
    public VisorTaskDescriptor(String taskCls, String[] argCls) {
        this.taskCls = taskCls;
        this.argCls = argCls != null ? argCls : EMPTY;
    }

    /**
     * @return Visor task class.
     */
    public String getTaskClass() {
        return taskCls;
    }

    /**
     * @return Visor task arguments classes.
     */
    public String[] getArgumentsClasses() {
        return argCls;
    }
}
