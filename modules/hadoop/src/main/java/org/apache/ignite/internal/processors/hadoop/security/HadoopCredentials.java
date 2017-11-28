package org.apache.ignite.internal.processors.hadoop.security;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Serializable;
import org.apache.hadoop.security.Credentials;
import java.io.ObjectOutputStream;

/**
 * Hadoop credentials.
 */
public class HadoopCredentials implements Serializable {
    /** */
    private static final long serialVersionUID = 0L;

    private Credentials ts;

    public HadoopCredentials(Credentials ts) {
        this.ts = ts;
    }

    public Credentials getCredentials() {
        return ts;
    }

    private void writeObject(ObjectOutputStream out) throws IOException {
        if (ts != null) {
            ts.write(out);
        }
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        this.ts = new Credentials();
        ts.readFields(in);
    }
}
