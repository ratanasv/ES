package com.es.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.cluster.health.ClusterHealthResponse;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.index.VersionType;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.es.client.ElasticClient;
import com.es.worker.ClearIndexWorker;

import static com.es.rax.RaxLocator.*;


public class IOHandler implements IOIface {
	private static final Logger log = Logger.getLogger(IOHandler.class);
	private static Client client = ElasticClient.getClient();
	// currently not that useful at the moment.
	private static final String ES_TYPE = "metrics";

	@Override
	public boolean insert(String tenantId, Map<String, String> map) {
		XContentBuilder content;
		try {
			content = createSourceContent(tenantId, map);
		} catch (IOException ie) {
			return false;
		}
		IndexResponse response = client.prepareIndex(getIndex(tenantId), ES_TYPE)
				//.setId(getId(content))
				.setId(getId(tenantId, map))
				.setRouting(getRouting(tenantId))
				.setSource(content)
				//.setVersion(1)
				//.setVersionType(VersionType.EXTERNAL)
				.execute()
				.actionGet();
		return true;
	}

	@Override
	public List<Map<String, Object>> search(String tenantId, Map<String, String> query) {
		List<Map<String, Object>> matched = new ArrayList<Map<String,Object>>();
    	SearchRequestBuilder request = createSearchRequest(tenantId, query);
    	SearchResponse searchRes = request.execute().actionGet();
    	for (SearchHit hit : searchRes.getHits().getHits()) {
    		log.debug("id=" + hit.getId() + ", shard=" + hit.getShard() + ", version=" + hit.version());
    		matched.add(hit.getSource());
    	}
    	return matched;
	}

	private SearchRequestBuilder createSearchRequest(String tenantId, Map<String, String> map) {
		SearchRequestBuilder request = client.prepareSearch(getIndex(tenantId))
			.setSize(500)
			.setRouting(getRouting(tenantId))
			.setVersion(true)
			.setQuery(QueryBuilders.fieldQuery(TENANT_ID.toString(), tenantId).analyzeWildcard(true));
		for(Map.Entry<String, String> entry : map.entrySet()) {
			request = request.setQuery(QueryBuilders.fieldQuery(entry.getKey(), entry.getValue()).analyzeWildcard(true));
		}
		return request;
	}
	
	private XContentBuilder createSourceContent(String tenantId, Map<String, String> map) throws IOException {
		XContentBuilder json = XContentFactory.jsonBuilder().startObject();
		// map might already contain tenantId already.
		if (!map.containsKey(TENANT_ID.toString()))
			json = json.field(TENANT_ID.toString(), tenantId);
		for (Map.Entry<String, String> entry : map.entrySet()) {
			json = json.field(entry.getKey(), entry.getValue());
		}
		json = json.endObject();
		return json;
	}

	private String getIndex(String tenantId) {
		return "test-index-" + String.valueOf(Math.abs(tenantId.hashCode() % 128));
	}

	private String getRouting(String tenantId) {
		return tenantId;
	}
	
	
	/**
	 * WARNING: XContentBuilder does not implement equals and hashCode, so this method is obsolete.
	 * @param content
	 * @return
	 */
	private String getId(XContentBuilder content) {
		return String.valueOf(content.hashCode());
	}
	
	/** Return the hashCode which can be used as the "id" of the arguments. 
	 * This is implemented since XContentBuilder does not.
	 * @param tenantId
	 * @param map
	 * @return hashCode (or "id").
	 */
	private String getId(String tenantId, Map<String, String> map) {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((map == null) ? 0 : map.hashCode());
		result = prime * result
				+ ((tenantId == null) ? 0 : tenantId.hashCode());
		return String.valueOf(result);
	}

}
