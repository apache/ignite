package org.apache.ignite.internal.processors.odbc;

import org.apache.ignite.internal.util.tostring.GridToStringInclude;
import org.apache.ignite.internal.util.typedef.internal.S;
import org.jetbrains.annotations.Nullable;

/**
 * ODBC protocol response.
 */
public class GridOdbcResponse {

    /** Command succeeded. */
    public static final int STATUS_SUCCESS = 0;

    /** Command failed. */
    public static final int STATUS_FAILED = 1;

    /** Success status. */
    @SuppressWarnings("RedundantFieldInitialization")
    private int successStatus = STATUS_SUCCESS;

    /** Error. */
    private String err;

    /** Response object. */
    @GridToStringInclude
    private Object obj;

    /**
     * Constructs successful rest response.
     *
     * @param obj Response object.
     */
    public GridOdbcResponse(Object obj) {
        successStatus = STATUS_SUCCESS;
        this.obj = obj;
    }

    /**
     * Constructs failed rest response.
     *
     * @param status Response status.
     * @param err Error, {@code null} if success is {@code true}.
     */
    public GridOdbcResponse(int status, @Nullable String err) {
        assert status != STATUS_SUCCESS;

        successStatus = status;
        this.err = err;
    }

    /**
     * @return Success flag.
     */
    public int getSuccessStatus() {
        return successStatus;
    }

    /**
     * @return Response object.
     */
    public Object getResponse() {
        return obj;
    }

    /**
     * @param obj Response object.
     */
    public void setResponse(@Nullable Object obj) {
        this.obj = obj;
    }

    /**
     * @return Error.
     */
    public String getError() {
        return err;
    }

    /**
     * @param err Error.
     */
    public void setError(String err) {
        this.err = err;
    }

    /** {@inheritDoc} */
    @Override public String toString() {
        return S.toString(GridOdbcResponse.class, this);
    }
}
