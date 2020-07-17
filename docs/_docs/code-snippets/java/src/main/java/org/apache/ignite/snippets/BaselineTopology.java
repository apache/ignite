package org.apache.ignite.snippets;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class BaselineTopology {

    void activate() {
        //tag::activate[]
        Ignite ignite = Ignition.start();

        ignite.cluster().active(true); 
        //end::activate[]
    }
    
    void enableAutoadjustment() {
        //tag::enable-autoadjustment[]
        
        Ignite ignite = Ignition.start();

        ignite.cluster().baselineAutoAdjustEnabled(true);
                
        ignite.cluster().baselineAutoAdjustTimeout(30000);

        //end::enable-autoadjustment[]

        //tag::disable-autoadjustment[]
        ignite.cluster().baselineAutoAdjustEnabled(false);
        //end::disable-autoadjustment[]

    }
}
