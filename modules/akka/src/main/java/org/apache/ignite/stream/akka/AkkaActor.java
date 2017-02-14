package org.apache.ignite.stream.akka;

import akka.actor.UntypedActor;

import java.util.HashMap;

/**
 * AkkaActor class only receive HashMap object and transmit to AkkaStreamer for further processing.
 */
public class AkkaActor extends UntypedActor {

    /**
     * The Akka Actor callback method for processing the Task
     * @see akka.actor.UntypedActor#onReceive(java.lang.Object)
     */
    @Override public void onReceive(Object message) throws Exception {
        if((message instanceof Object)) {
            new AkkaStreamer((HashMap) message);
        }
    }
}
