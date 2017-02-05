package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.transport.client.PreBuiltTransportClient;

import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineBasicInterface;

public class ElasticSearchEngineBasic implements SearchEngineBasicInterface{
	
	private TransportClient esClient = null;
	
	public ElasticSearchEngineBasic(Map<String, Object> serverParams) {
		
		try {
			
			// 读取Elasticsearch服务器的IP地址配置
			byte[] serverAddress = (byte [])serverParams.get("server");
			// 读取Elasticsearch服务器的端口配置
			int port = (int)serverParams.get("port");
			// 读取集群名称
			String cluster = serverParams.get("cluster").toString();
			
			// 根据上述配置，初始化Elasticsearch客户端
			esClient = new PreBuiltTransportClient(Settings.builder().put("cluster.name", cluster).build())  
					.addTransportAddress(new InetSocketTransportAddress(InetAddress.getByAddress(serverAddress), port));
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
			
	}
	
	public void cleanup() {
		
		// 关闭TransportClient的连接
		if (esClient != null) {
			esClient.close();
			esClient = null;
		}
		
	}
	
	
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		IndexResponse response = null;
		
		// 适配部分：根据输入的统一文档ListingDocument，生成并添加Elasticsearch所使用的HashMap
		for (ListingDocument ld : documents) {
			
			String indexName = indexParams.get("index").toString();
			String typeName = indexParams.get("type").toString();
			
			Map<String, Object> fieldsMap = new HashMap<>();
			fieldsMap.put("listing_id", ld.getListing_id());
			fieldsMap.put("listing_title", ld.getListing_title());
			fieldsMap.put("category_id", ld.getCategory_id());
			fieldsMap.put("category_name", ld.getListing_id());
			
			// 写入集群的索引
			response = esClient.prepareIndex(indexName, typeName)  
	                .setSource(fieldsMap)  
	                .get();  
			
		}
		
		return response.toString();
	}
	
	public String query(Map<String, Object> queryParams) {
		// TODO Auto-generated method stub
		
		SearchResponse response = null;
		
		try {
			
			// 适配部分：根据输入的搜索请求，生成Elasticsearch所能识别的查询
			String indexName = queryParams.get("index").toString();
			String typeName = queryParams.get("type").toString();
			String query = queryParams.get("query").toString();
			String[] fields = (String []) queryParams.get("fields");
			int from = (int)(queryParams.get("from"));
			int size = (int)(queryParams.get("size"));
			String mode = queryParams.get("mode").toString();
	
			QueryBuilder qb = null;
			if ("MultiMatchQuery".equalsIgnoreCase(mode)) {
				// 基础查询的构造，默认使用了OR的布尔操作，相关性较低
				qb = QueryBuilders.multiMatchQuery(query, fields);
			} else {
				// 更好的查询的构造，采用AND的布尔操作，提升相关性
				String[] terms = query.split("\\s+");
				for (String term : terms) {
					if (qb == null) {
						qb = QueryBuilders.boolQuery()
								.must(QueryBuilders.multiMatchQuery(term, fields));
					} else {
						qb = QueryBuilders.boolQuery()
								.must(qb)
								.must(QueryBuilders.multiMatchQuery(term, fields));
					}
				}
			}
			
			// 获取查询结果
			response = esClient.prepareSearch(indexName).setTypes(typeName)
					.setSearchType(SearchType.DEFAULT)
					.setQuery(qb)
					.setFrom(from).setSize(size)
					.get();  
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
		
		return response.toString();
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// Elasticsearch服务器的设置，根据你的需要设置
		Map<String, Object> serverParams = new HashMap<>();
		serverParams.put("server",  new byte[]{(byte)192,(byte)168,1,48});
		serverParams.put("port", 9300);
		serverParams.put("cluster", "ECommerce");
		//初始化
		ElasticSearchEngineBasic ese = new ElasticSearchEngineBasic(serverParams);
		
		
		// 测试索引接口
		Map<String, Object> indexParams = new HashMap<>();
		indexParams.put("index", "listing_new_byclient");
		indexParams.put("type", "listing");
		ListingDocument ld1 = new ListingDocument(
				1001, "ES客户端索引测试标题1", 100001, "ES客户端索引测试类目1");
		ListingDocument ld2 = new ListingDocument(
				1002, "ES客户端索引测试标题2", 100002, "ES客户端索引测试类目2");
		List<ListingDocument> documents = new ArrayList<>();
		documents.add(ld1);
		documents.add(ld2);
		// 索引测试文档
		System.out.println(ese.index(documents, indexParams));
		
		// 测试查询接口
		Map<String, Object> queryParams = new HashMap<>();
		// 和Solr有所不同，需要在这里指定索引和类型
		queryParams.put("index", "listing_new");
		queryParams.put("type", "listing");
		// 查询关键词
		queryParams.put("query", "西红柿 方便面");
		// 在两个字段上查询
		queryParams.put("fields", new String[] {"listing_title", "category_name"});
		queryParams.put("from", 0);	// 从第1条结果记录开始
		queryParams.put("size", 5);	// 返回5条结果记录
		queryParams.put("mode", "MultiMatchQuery");	// 选择基础查询模式
		System.out.println(ese.query(queryParams));	// 查询并输出
		
		queryParams.put("mode", "BoolQuery");	// 选择优化后的查询模式
		System.out.println(ese.query(queryParams));	// 再次查询并输出
		
		
		ese.cleanup();
		
	}

}
