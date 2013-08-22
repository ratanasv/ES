package com.es.client;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;

public final class ClientManager {
	private static Client client = null;
	private static Settings settings = ImmutableSettings.settingsBuilder()
			.put("client.transport.ignore_cluster_name", true)
			.build();
	
	static enum Hosts {
		VIR_NODE("192.237.162.207"),
		DFW1_ES("50.56.179.39"),
		ORD1_ES("162.209.4.236");
		Hosts(String in) {this.host = in;}
		private String host;
		String get() {return this.host;}
	}
	
	static {
		client = new TransportClient(settings)
			.addTransportAddress(new InetSocketTransportAddress(Hosts.DFW1_ES.get(), 9300));
	}
	
	private ClientManager() {
		
	}
	
	public static Client getClient() {
		return client;
	}

}
