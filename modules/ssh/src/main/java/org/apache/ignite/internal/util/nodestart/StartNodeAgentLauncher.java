package org.apache.ignite.internal.util.nodestart;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteLogger;
import org.apache.ignite.cluster.ClusterStartNodeResult;

import org.apache.ignite.internal.cluster.ClusterStartNodeResultImpl;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.resources.LoggerResource;


public class StartNodeAgentLauncher implements StartNodeCallable{
	
	/** Logger. */
    @LoggerResource
    private transient IgniteLogger log;

    /** Ignite. */
    @IgniteInstanceResource
    private transient Ignite ignite;
    
    /** Specification. */
    private final IgniteRemoteStartSpecification spec;
    /** Connection timeout. */
    private final int timeout;
	
	 /**
     * Required by Externalizable.
     */
    public StartNodeAgentLauncher() {
        spec = null;

        timeout = 0;

        assert false;
    }

    /**
     * Constructor.
     *
     * @param spec Specification.
     * @param timeout Connection timeout.
     */
    public StartNodeAgentLauncher(IgniteRemoteStartSpecification spec, int timeout) {
        assert spec != null;

        this.spec = spec;

        this.timeout = timeout;
    }
    
	@Override
	public ClusterStartNodeResult call() throws Exception {
		// start node by agent
		ClusterStartNodeResult result = new ClusterStartNodeResultImpl(spec.host(),false,"not implement!");
		return result;
	}
	
	

}
