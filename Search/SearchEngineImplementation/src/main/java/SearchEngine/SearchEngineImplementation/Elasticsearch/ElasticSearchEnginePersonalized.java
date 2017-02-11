package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEnginePersonalizedInterface;
import SearchEngine.UserProfile.UserProfile;

public class ElasticSearchEnginePersonalized implements SearchEnginePersonalizedInterface{
	
	private ElasticSearchEngineRelevant eser = null;
	
	public ElasticSearchEnginePersonalized(ElasticSearchEngineRelevant eser) {
		
		// 相关性搜索引擎不变
		this.eser = eser;
		
		// 增加了用户画像获取模块
		UserProfile.init();
			
	}
	
	public void cleanup() {
		
		eser.cleanup();
		UserProfile.cleanup();	// 增加了用户画像的资源回收
		
	}
	
	// 索引部分保持不变
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		return eser.index(documents, indexParams);
		
	}
	
	// 查询部分附加上个性化的逻辑
	public String query(Map<String, Object> queryParams, String userid) {
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
			
			// 新增的装饰部分：根据用户ID userid，获取该用户的喜好数据
			HashMap<String, String> preference = 
					(HashMap<String, String>) UserProfile.getPrefence("user_profile", userid);
			
			// 对于分类的喜好程度，通过category_name字段的boost实现
			if (preference.containsKey("categories")) {
				String[] categories = preference.get("categories").split("[，|,]");
				for (String category : categories) {
					qb = QueryBuilders.boolQuery()
							.must(qb)
							.should(QueryBuilders.matchQuery("category_name", category).boost(0.1f));
												// 可以根据个性化的程度、应用的需要来调整这里0.1的得分
				}
			}
			
			// 由于数据有限，目前关于品牌的喜好是通过商品标题字段listing_title实现的
			if (preference.containsKey("brands")) {
				String[] brands = preference.get("brands").split("[，|,]");
				for (String brand : brands) {
					qb = QueryBuilders.boolQuery()
							.must(qb)
							.should(QueryBuilders.matchQuery("listing_title", brand).boost(0.1f));
												// 可以根据个性化的程度、应用的需要来调整这里0.1的得分
				}
			}
			
			// 由于数据有限，目前关于标签也是通过商品标题字段listing_title来实现的
			if (preference.containsKey("tags")) {
				String[] tags = preference.get("tags").split("[，|,]");
				for (String tag : tags) {
					qb = QueryBuilders.boolQuery()
							.must(qb)
							.should(QueryBuilders.matchQuery("listing_title", tag).boost(0.1f));
												// 可以根据个性化的程度、应用的需要来调整这里0.1的得分
				}
			}
			
			// 之前的装饰部分：通过查询分类的结果，优化相关性
			// 如下这行可以使用RESTful API或者服务化模块代替，这样模块间耦合度更低
			HashMap<String, Double> queryClassificationResults 
				= (HashMap<String, Double>) eser.nbqcsearch.predict(query);
			
			for (String cate : queryClassificationResults.keySet()) {
				float score = queryClassificationResults.get(cate).floatValue();
				if (score < 0.02) continue;		// 去除得分过低的噪音点
				
				qb = QueryBuilders.boolQuery()
						.must(qb)
						.should(QueryBuilders.matchQuery("category_name", cate).boost(score));
				
			}
						
			
			// 获取查询结果
			response = eser.eseb.esClient.prepareSearch(indexName).setTypes(typeName)
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
		
		//初始化三个基于Elasticsearch的搜索引擎，一个是基础款，一个是相关性改善后的，最后一个是个性化的
		ElasticSearchEngineBasic eseb = new ElasticSearchEngineBasic(serverParams);
		ElasticSearchEngineRelevant eser = new ElasticSearchEngineRelevant(eseb);
		ElasticSearchEnginePersonalized esep = new ElasticSearchEnginePersonalized(eser);
		
		
		// 测试查询接口
		Map<String, Object> queryParams = new HashMap<>();
		// 和Solr有所不同，需要在这里指定索引和类型
		queryParams.put("index", "listing_new");
		queryParams.put("type", "listing");
		// 查询关键词
		queryParams.put("fields", new String[] {"listing_title"});
		queryParams.put("from", 0);	// 从第1条结果记录开始
		queryParams.put("size", 10);	// 返回5条结果记录
		queryParams.put("mode", "BoolQuery");	// 选择基础查询模式
		
		
		// 对比基础搜索、相关性改善后的搜索、以及个性化搜索
		String[] queries = {"牛奶", "手机", "康师傅", "苹果"};
		LinkedHashMap<String, String> users = new LinkedHashMap<>();
		users.put("user1", "张三");
		users.put("user2", "李四");
		users.put("user3", "王五");
		users.put("user4", "赵六");
		for (String query : queries) {
			
			System.out.println("查询——" + query);
			
			queryParams.put("query", query);
			System.out.println("基础搜索：\t\t" + eseb.query(queryParams));
			System.out.println("相关性改良后的搜索：\t" + eser.query(queryParams));
			for (String userid : users.keySet()) {
				System.out.println(String.format("%s用户个性化的搜索：\t%s", users.get(userid), esep.query(queryParams, userid)));
			}
			
			System.out.println("***********************");
			System.out.println();
		}
		
		esep.cleanup();
		
	}

}
