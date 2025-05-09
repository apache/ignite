package org.apache.ignite.console.agent.service;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.ignite.Ignite;
import org.apache.ignite.internal.plugin.IgniteVertxPlugin;
import org.apache.ignite.internal.plugin.IgniteWebSocketPlugin;
import org.apache.ignite.resources.IgniteInstanceResource;


import io.swagger.annotations.ApiOperation;
import io.vertx.core.DeploymentOptions;
import io.vertx.core.Future;
import io.vertx.core.Promise;
import io.vertx.core.Verticle;
import io.vertx.core.Vertx;
import io.vertx.core.impl.Deployment;
import io.vertx.core.impl.VertxInternal;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.core.spi.VerticleFactory;

@ApiOperation(value = "Deploy Verticle to the cluster", notes = "部署Verticle到vertx集群中。参数(op:list|deploy|undeploy|cancel,verticles:deploymentIds)")
public class ClusterAgentVerticleManager implements ClusterAgentService {

	@IgniteInstanceResource
	private Ignite ignite;

	private IgniteVertxPlugin vertxPlugin;
	
	public ClusterAgentVerticleManager() {
		
	}

	public ClusterAgentVerticleManager(Ignite ignite) {
		this.ignite = ignite;
	}

	@Override
	public ServiceResult call(String cluterId, Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		JsonObject args = new JsonObject(payload);
		String serviceName = args.getString("op", "list");
		vertxPlugin = ignite.plugin("Vertx");
		if (vertxPlugin == null) {
			result.addMessage("Vertx Plugin not found! Please start vertx cluster first!");
			return result;
		}
		if (serviceName.equals("list")) {
			result = verticleList(args.getMap());
		} else if (serviceName.equals("deploy")) {
			result = deployVerticle(args.getMap());
		} else if (serviceName.equals("undeploy")) {
			result = undeployVerticle(args.getMap());
		} else if (serviceName.equals("cancel")) {
			result = cancelVerticle(args.getMap());
		} else {
			result.addMessage("Unsupport op “" + serviceName + "“");
		}

		return result;
	}

	public ServiceResult verticleList(Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		VertxInternal vertx = (VertxInternal)vertxPlugin.vertx();
		for (String id : vertx.deploymentIDs()) {
			
			Deployment deployment = vertx.getDeployment(id);
			DeploymentOptions options = deployment.deploymentOptions();
			JsonObject info = JsonObject.mapFrom(options);
			info.put("name", "<unk>");
			for(Verticle v: deployment.getVerticles()) {
				info.put("name", v.getClass().getSimpleName());
				ApiOperation api = v.getClass().getAnnotation(ApiOperation.class);				

				if (api != null) {
					info.put("description", api.value());
					info.put("notes", api.notes());
				} else {
					info.put("description", v.getClass().getName());
					info.put("notes", "");
				}
				break;
			}
			
			result.put(id, info);
		}
		return result;
	}

	public ServiceResult deployVerticle(Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		Vertx vertx = vertxPlugin.getVertx();
		JsonObject args = new JsonObject(payload);
		JsonArray services = args.getJsonArray("verticlesClass");
		List<String> messages = new ArrayList<>();
		for (Object service : services) {
			if (service instanceof String) {
				DeploymentOptions options = new DeploymentOptions();
				try {
					Class<? extends Verticle> ctx = (Class) Class.forName(service.toString());

					ApiOperation api = ctx.getAnnotation(ApiOperation.class);
					JsonObject info = JsonObject.mapFrom(options);

					if (api != null) {
						info.put("description", api.value());
						info.put("notes", api.notes());
					} else {
						info.put("description", ctx.getName());
						info.put("notes", "");
					}
					info.put("name", ctx.getSimpleName());

					String deployId = vertx.deployVerticle(ctx, options).result();					
					
					result.put(deployId, info);

				} catch (RuntimeException e) {
					messages.add(e.getMessage());
				} catch (ClassNotFoundException e) {
					messages.add(e.getMessage());
				}
			}
		}
		return result;
	}

	public ServiceResult undeployVerticle(Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		JsonObject args = new JsonObject(payload);
		Vertx vertx = vertxPlugin.getVertx();
		JsonArray services = args.getJsonArray("verticles");
		List<String> messages = result.messages;
		for (Object service : services) {
			if (service instanceof String) {
				String deploymentID = service.toString();
				JsonObject info = new JsonObject();
				info.put("id", deploymentID);
				try {
					vertx.undeploy(deploymentID);

				} catch (Exception e) {
					messages.add(e.getMessage());
				}
				result.put(deploymentID, info);
			}
		}
		return result;
	}

	public ServiceResult cancelVerticle(Map<String, Object> payload) {
		ServiceResult result = new ServiceResult();
		JsonObject args = new JsonObject(payload);
		VertxInternal vertx = (VertxInternal)vertxPlugin.getVertx();
		JsonArray services = args.getJsonArray("verticles");
		List<String> messages = result.messages;
		for (Object service : services) {
			if (service instanceof String) {
				String deploymentID = service.toString();
				JsonObject info = new JsonObject();
				info.put("id", deploymentID);
				try {
					Deployment deployment = vertx.getDeployment(deploymentID);
					for(Verticle v: deployment.getVerticles()) {
						v.stop(Promise.promise());
					}

				} catch (Exception e) {
					messages.add(e.getMessage());
				}
				result.put(deploymentID, info);
			}
		}
		return result;
	}
}
