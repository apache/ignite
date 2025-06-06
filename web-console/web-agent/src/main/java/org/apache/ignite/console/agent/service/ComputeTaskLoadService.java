

package org.apache.ignite.console.agent.service;

import java.util.HashMap;
import java.util.Map;

import org.apache.ignite.Ignite;
import org.apache.ignite.compute.ComputeTask;
import org.apache.ignite.compute.ComputeTaskCancelledException;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.ignite.scheduler.SchedulerFuture;

import io.swagger.annotations.ApiOperation;
import io.vertx.core.json.JsonObject;

/**
 * Demo service. Run tasks on nodes. Run demo load on caches.
 */
@ApiOperation(value="定时执行集群任务",notes="这个操作可以设置周期性调用")
public class ComputeTaskLoadService implements ClusterAgentService {
    /** Ignite instance. */
    @IgniteInstanceResource
    private Ignite ignite;
    
	@Override
	public ServiceResult call(String cluterId,Map<String, Object> payload) {		
		ServiceResult result = new ServiceResult();
		int count = 0;
		JsonObject args = new JsonObject(payload);	
		String task = args.getString("task");
		if(task.indexOf('.')<0) {
			task = "org.apache.ignite.console.agent.task."+task;
		}
		
		String cronString = args.getString("cronString");	
		
		
		try {
			final Class<? extends ComputeTask<JsonObject, JsonObject>> taskClass = (Class)Class.forName(task);
			
			Runnable job = new Runnable(){
	           
				@Override
				public void run() {
					try {
		                ignite.compute().withNoFailover()
		                    .execute(taskClass, args);
		            }
		            catch (ComputeTaskCancelledException ignore) {
		                // No-op.
		            }
		            catch (Throwable e) {
		                ignite.log().error("DemoCancellableTask execution error", e);
		                result.addMessage(e.getMessage());
		            }
					
				}
	        };
			
	        SchedulerFuture<?> future = ignite.scheduler().scheduleLocal(job, cronString);
	        
		} catch (ClassNotFoundException e1) {
			
			e1.printStackTrace();			
			result.addMessage(e1.getMessage());
		}		
		
		return result;
	}
}
