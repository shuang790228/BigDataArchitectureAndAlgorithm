package SearchEngine.SearchEngineImplementation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import SearchEngine.SearchEngineImplementation.Elasticsearch.ElasticSearchEngineBasic;
import SearchEngine.SearchEngineImplementation.Solr.SolrSearchEngineBasic;

public class SearchEngineTest {
	
	private static SolrSearchEngineBasic sse = null;
	private static ElasticSearchEngineBasic ese = null;
	
	public static synchronized void init() {
		
		if (sse == null) {
			Map<String, Object> serverParams = new HashMap<>();
			// 连接Solr Cloud的Zookeeper设置，根据你的需要设置
			serverParams.put("zkHost",  "192.168.1.48:9983");
			// 索引写入哪个Collection，根据你的需要设置。这里写入另一个测试的collection
			serverParams.put("collection",  "listing_collection");
			// 初始化
			sse = new SolrSearchEngineBasic(serverParams);
		}
		
		if (ese == null) {
			// Elasticsearch服务器的设置，根据你的需要设置
			Map<String, Object> serverParams = new HashMap<>();
			serverParams.put("server",  new byte[]{(byte)192,(byte)168,1,48});
			serverParams.put("port", 9300);
			serverParams.put("cluster", "ECommerce");
			//初始化
			ese = new ElasticSearchEngineBasic(serverParams);
		}
		
	}
	
	public static void index(List<ListingDocument> documents) {
		
		// 同时索引测试文档到Solr和Elasticsearch两个集群
		// 这对调用方是不可见的，因此逻辑修改不影响调用方
		
		// 索引到Solr集群
		Map<String, Object> indexParams = new HashMap<>();
		sse.index(documents, indexParams);
		
		// 索引到Elasticsearch集群
		indexParams.clear();
		indexParams.put("index", "listing_new");
		indexParams.put("type", "listing");
		ese.index(documents, indexParams);
		
	}
	
	public static List<ListingDocument> query(String keywords, int page, int number) {
		
		// 随机选取Solr和Elasticsearch集群中的一个进行服务
		// 这对调用方是不可见的，因此逻辑修改不影响调用方
		
		List<ListingDocument> results = new ArrayList<ListingDocument>();
		
		int start = (page - 1) * number;		//假设page从1开始计数
		int rows = number;
		
		long timeMills = System.currentTimeMillis();
		if (timeMills % 2 == 0) {		// 使用Solr服务该请求
			Map<String, Object> queryParams = new HashMap<>();
			// 查询关键词
			queryParams.put("query", keywords);
			// 在两个字段上查询
			queryParams.put("fields", 
					new String[] {"listing_title", "category_name"});
			queryParams.put("start", start);	// 从第1条结果记录开始
			queryParams.put("rows", rows);		// 返回5条结果记录
			String response = sse.query(queryParams);	// 查询结果
			
			// 解析Solr返回的结果，并封装成统一的ListingDocument
			// 感兴趣的读者可以自行实现
			/*for (...) {
			 *ListingDocument ld = new ...
				results.add(ld)...
			}*/
			
			
		} else {		// 使用Elasticsearch服务该请求
			
			Map<String, Object> queryParams = new HashMap<>();
			// 和Solr有所不同，需要在这里指定索引和类型
			queryParams.put("index", "listing_new");
			queryParams.put("type", "listing");
			// 查询关键词
			queryParams.put("query", keywords);
			// 在两个字段上查询
			queryParams.put("fields", new String[] {"listing_title", "category_name"});
			queryParams.put("from", start);	// 从第1条结果记录开始
			queryParams.put("size", rows);	// 返回5条结果记录
			queryParams.put("mode", "BoolQuery");	// 选择优化后的查询模式
			String response = ese.query(queryParams);	// 查询结果
			
			// 解析Elasticsearch返回的结果，并封装成统一的ListingDocument
			// 感兴趣的读者可以自行实现
			/*for (...) {
			 *ListingDocument ld = new ...
				results.add(ld)...
			}*/
			
		}
		
		return results;
	}
	
	public static void cleanup() {
		
		if (sse != null) {
			sse.cleanup();
			sse = null;
		}
		
		if (ese != null) {
			ese.cleanup();
			ese = null;
		}
		
	}
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		SearchEngineTest.init();
		
		ListingDocument ld1 = new ListingDocument(
				1001, "索引测试标题1", 100001, "索引测试类目1");
		ListingDocument ld2 = new ListingDocument(
				1002, "索引测试标题2", 100002, "索引测试类目2");
		List<ListingDocument> documents = new ArrayList<>();
		documents.add(ld1);
		documents.add(ld2);
		
		// 搜索引擎内部具体实现发生变化时，外部应用程序的调用可以保持不变
		SearchEngineTest.index(documents);	//索引新文档
		SearchEngineTest.query("西红柿 方便面", 2, 5);	//搜索第2页，每页5项结果
		
		SearchEngineTest.cleanup();
	}
	

}
