package com.es.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public final class ClientManager {
	private static Settings settings = ImmutableSettings.settingsBuilder()
			.put("client.transport.ignore_cluster_name", true)
			.build();
	private static Hosts defaultHost = Hosts.DFW1_ES;

	public static enum Hosts {
		VIR_NODE("192.237.162.207"),
		DFW1_ES("50.56.179.39"),
		ORD1_ES("162.209.4.236");

		private Client client;

		private Hosts(String in) {
			this.host = in;
			this.client = new TransportClient(settings)
				.addTransportAddress(new InetSocketTransportAddress(this.getHost(), 9300));
			
		}
		private String host;
		String getHost() {return this.host;}
		Client getClient() {
			return this.client;
		}
	}

	private ClientManager() {

	}

	public static Client getClient() {
		return defaultHost.getClient();
	}
	
	public static Client getClient(Hosts host) {
		return host.getClient();
	}

}
