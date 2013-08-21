package com.es.api;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

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
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;

import com.es.client.ElasticClient;
import static com.es.type.RaxLocator.*;


public class IOHandler implements IOIface {

	private static Client client = ElasticClient.getClient();
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
				.setRouting(getRouting(tenantId))
				.setSource(content)
				.execute()
				.actionGet();
		return true;
	}

	@Override
	public List<Map<String, Object>> search(String tenantId, Map<String, String> query) {
		List<Map<String, Object>> matched = new ArrayList<Map<String,Object>>();
    	SearchRequestBuilder request = createSearchRequest(tenantId, query);
    	SearchResponse searchRes = request.execute().actionGet();
    	searchRes.getHits().getHits().toString();
    	for (SearchHit hit : searchRes.getHits().getHits()) {
    		matched.add(hit.getSource());
    	}
    	return matched;
	}

	private SearchRequestBuilder createSearchRequest(String tenantId, Map<String, String> map) {
		SearchRequestBuilder request = client.prepareSearch(getIndex(tenantId))
			.setRouting(getRouting(tenantId))
			.setQuery(QueryBuilders.fieldQuery(TENANT_ID.toString(), tenantId).analyzeWildcard(true));
		for(Map.Entry<String, String> entry : map.entrySet()) {
			request = request.setQuery(QueryBuilders.fieldQuery(entry.getKey(), entry.getValue()).analyzeWildcard(true));
		}
		return request;
	}
	
	private XContentBuilder createSourceContent(String tenantId, Map<String, String> map) throws IOException {
		XContentBuilder json = XContentFactory.jsonBuilder().startObject().field(TENANT_ID.toString(), tenantId);
		for(Map.Entry<String, String> entry : map.entrySet()) {
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

}
