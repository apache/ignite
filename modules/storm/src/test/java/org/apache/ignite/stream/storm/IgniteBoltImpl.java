package org.apache.ignite.stream.storm;

import backtype.storm.tuple.Tuple;

import java.util.Map;

/**
 * @author  Gianfranco Murador
 *
 */
public class IgniteBoltImpl extends  IgniteBolt {
    /**
     * This method should be overridden by providing a way to translate a tuple in a map
     * @param tuple
     * @return The format accepted by Ignite
     * @throws Exception
     */
    @Override public Map<?, ?> process(Tuple tuple) throws Exception {
        return null;
    }
}
