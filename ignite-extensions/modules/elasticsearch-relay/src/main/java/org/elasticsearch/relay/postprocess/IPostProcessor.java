package org.elasticsearch.relay.postprocess;

import java.util.Set;

import com.fasterxml.jackson.databind.node.ObjectNode;


/**
 * Interface for a result post processor for individual result entries.
 */
public interface IPostProcessor {
	/**
	 * Processes a result object, possibly modifying it in the process.
	 * 
	 * @param result
	 *            result object to process
	 * @return modified result object
	 * @throws Exception
	 *             if processing fails
	 */
	public ObjectNode process(ObjectNode result) throws Exception;
	
	/**
	 * 返回支持的type集合。返回null代表全局，返回empty代表什么都不处理
	 * @return
	 */
	public Set<String> getTypeSet();
}
