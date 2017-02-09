package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import MahoutMachineLearning.Classification.NBQueryClassifierOnlineForSearch;
import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineBasicInterface;

public class ElasticSearchEngineRelevant implements SearchEngineBasicInterface{
	
	private ElasticSearchEngineBasic eseb = null;
	private NBQueryClassifierOnlineForSearch nbqcsearch = null; 
	
	public ElasticSearchEngineRelevant(ElasticSearchEngineBasic eseb) {
		
		// 基本的搜索引擎不变
		this.eseb = eseb;
		
		// 增加了查询分类模块
		nbqcsearch = new NBQueryClassifierOnlineForSearch();
			
	}
	
	public void cleanup() {
		
		eseb.cleanup();
		nbqcsearch.cleanup();	// 增加了查询分类的资源回收
		
	}
	
	// 索引部分保持不变
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		return eseb.index(documents, indexParams);
		
	}
	
	// 查询部分附加上相关性的逻辑
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
			
			qb = QueryBuilders.boolQuery()
					.must(QueryBuilders.matchAllQuery());
			
			// 更好的查询的构造，采用AND的布尔操作，提升相关性
			String[] terms = query.split("\\s+");
			for (String term : terms) {
				qb = QueryBuilders.boolQuery()
						.must(qb)
						.filter(QueryBuilders.multiMatchQuery(term, fields));
			}
			
			// 新增的装饰部分：通过查询分类的结果，优化相关性
			// 如下这行可以使用RESTful API或者服务化模块代替，这样模块间耦合度更低
			HashMap<String, Double> queryClassificationResults 
				= (HashMap<String, Double>) nbqcsearch.predict(query);
			
			for (String cate : queryClassificationResults.keySet()) {
				float score = queryClassificationResults.get(cate).floatValue();
				if (score < 0.02) continue;		// 去除得分过低的噪音点
				
				qb = QueryBuilders.boolQuery()
						.must(qb)
						.should(QueryBuilders.matchQuery("category_name", cate).boost(score));
				
			}
						
			
			// 获取查询结果
			response = eseb.esClient.prepareSearch(indexName).setTypes(typeName)
					.setSearchType(SearchType.DEFAULT)
					.setQuery(qb)
					.setFrom(from).setSize(size)
					.get();  
			
//			。。。这里略去后续统一文档拼装的实现。。。

			
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
		
		//初始化两个基于Elasticsearch的搜索引擎，一个是基础款，一个是相关性改善后的
		ElasticSearchEngineBasic eseb = new ElasticSearchEngineBasic(serverParams);
		ElasticSearchEngineRelevant eser = new ElasticSearchEngineRelevant(eseb);
		
		
		// 测试查询接口
		Map<String, Object> queryParams = new HashMap<>();
		// 和Solr有所不同，需要在这里指定索引和类型
		queryParams.put("index", "listing_new");
		queryParams.put("type", "listing");
		// 查询关键词
		queryParams.put("query", "米");
		queryParams.put("fields", new String[] {"listing_title"});
		queryParams.put("from", 0);	// 从第1条结果记录开始
		queryParams.put("size", 5);	// 返回5条结果记录
		queryParams.put("mode", "BoolQuery");	// 选择基础查询模式
		
		// 对比相关性改善前后
		System.out.println("基础搜索：\t\t" + eseb.query(queryParams));	// 查询并输出
		System.out.println("相关性改良后的搜索：\t" + eser.query(queryParams));	// 再次查询并输出
		
		eseb.cleanup();
		eser.cleanup();
		
	}

}
