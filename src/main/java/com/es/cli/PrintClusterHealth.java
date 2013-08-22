package com.es.cli;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.admin.cluster.state.ClusterStateResponse;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.collect.UnmodifiableIterator;

import com.es.client.ClientManager;

public class PrintClusterHealth {
	/**
	 * @param args
	 * @throws InterruptedException 
	 * @throws IOException 
	 */
	public static void main(String[] args) throws InterruptedException, IOException {
		
		while(true) {
			Runtime.getRuntime().exec("printf \"\033c\"");
			ClusterHealthResponse healthRes = ClientManager.getClient()
					.admin().cluster().prepareHealth()
					.execute().actionGet();
			Thread.sleep(1000);
			System.out.println(healthRes.getStatus().toString() + "\tnodes=" + healthRes.getNumberOfNodes() + 
				"\tactive=" + healthRes.getActiveShards() + "\tiniting=" + healthRes.getInitializingShards() + 
				"\tunassigned=" + healthRes.getUnassignedShards() + "\trelocating=" + healthRes.getRelocatingShards());
			ClusterStateResponse stateRes = ClientManager.getClient().admin().cluster().prepareState().execute().actionGet();
			Map<String, String> idToName = new HashMap<String, String>();
			for(DiscoveryNode node : stateRes.getState().nodes().getDataNodes().values() ){
				idToName.put(node.getId(), node.getName());
			}
			
			for (Map.Entry<String, String> entry : idToName.entrySet()) {
				int numShards = stateRes.getState().getRoutingNodes().node(entry.getKey()).numberOfOwningShards();
				System.out.print(entry.getValue() + " : " + numShards + " ");
			}
			System.out.print("\n ------------------------ \n");
		}
	}

}
