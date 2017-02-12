package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.search.SearchType;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

import MahoutMachineLearning.Classification.NBQueryClassifierOnlineForSearch;
import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineBasicInterface;

public class ElasticSearchEngineRelevantWithRoute implements SearchEngineBasicInterface{
	
	protected ElasticSearchEngineBasic eseb = null;
	protected NBQueryClassifierOnlineForSearch nbqcsearch = null; 
	private HashMap<String, String> category2route = new HashMap<>();
	
	public ElasticSearchEngineRelevantWithRoute(ElasticSearchEngineBasic eseb) {
		
		// 基本的搜索引擎不变
		this.eseb = eseb;
		
		// 增加了查询分类模块
		nbqcsearch = new NBQueryClassifierOnlineForSearch();
		
		// 初始化分类到路由的映射
		// 消费电子
		category2route.put("手机", "ce");
		category2route.put("电脑", "ce");
		// 日用品
		category2route.put("美发护发", "daily");
		category2route.put("沐浴露", "daily");
		category2route.put("大米", "daily");
		category2route.put("食用油", "daily");
		category2route.put("面粉", "daily");
		
		// 饮料和零食
		category2route.put("坚果", "drinksnack");
		category2route.put("巧克力", "drinksnack");
		category2route.put("饼干", "drinksnack");
		category2route.put("饮料饮品", "drinksnack");
		category2route.put("方便面", "drinksnack");
		
		// 生鲜和干货
		category2route.put("海鲜水产", "freshdry");
		category2route.put("新鲜水果", "freshdry");
		category2route.put("纯牛奶", "freshdry");
		category2route.put("进口牛奶", "freshdry");
		category2route.put("枣类", "freshdry");
		category2route.put("茶叶", "freshdry");
			
	}
	
	public void cleanup() {
		
		eseb.cleanup();
		nbqcsearch.cleanup();	// 增加了查询分类的资源回收
		
	}
	
	// 索引部分加入了route路由机制
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
			
			// 写入集群的索引，增加路由的设置
			response = eseb.esClient.prepareIndex(indexName, typeName)
					.setRouting(category2route.get(ld.getCategory_name()))
	                .setSource(fieldsMap)  
	                .get();  
			
		}
		
		return response.toString();
		
	}
	
	// 查询部分附加上相关性、以及基于路由的逻辑
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
			
			ArrayList<String> routing = new ArrayList<>();
			
			for (String cate : queryClassificationResults.keySet()) {
				float score = queryClassificationResults.get(cate).floatValue();
				if (score < 0.02) continue;		// 去除得分过低的噪音点
				
				qb = QueryBuilders.boolQuery()
						.must(qb)
						.should(QueryBuilders.matchQuery("category_name", cate).boost(score));
				
				String route = category2route.get(cate);
				if (!routing.contains(route)) {
					routing.add(route);
				}
			}
						
			
			String[] routes = (String[]) routing.toArray(new String[routing.size()]);
			
			// 获取查询结果
			response = eseb.esClient.prepareSearch(indexName).setTypes(typeName)
					.setRouting(routes)
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
	
	public void indexListing(String file, Map<String, Object> indexParams) {
		
		ArrayList<ListingDocument> documents = new ArrayList<>();
		
		try {
			
			// 读取原始Listing商品数据文件，拼装ListingDocument
			BufferedReader br = new BufferedReader(new FileReader(file));
			String strLine = null;
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				
				ListingDocument ld = new ListingDocument(Long.parseLong(tokens[0]), tokens[1], 
						Long.parseLong(tokens[2]), tokens[3]);
				documents.add(ld);
			}
			
			br.close();
			
			System.out.println("start to index...");
			this.index(documents, indexParams);
			System.out.println("finished");
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		// Elasticsearch服务器的设置，根据你的需要设置
		Map<String, Object> serverParams = new HashMap<>();
//		serverParams.put("server",  new byte[]{(byte)192,(byte)168,1,48});
		serverParams.put("server",  new byte[]{127,0,0,1});
		serverParams.put("port", 9300);
		serverParams.put("cluster", "ECommerce");
		serverParams.put("index", "listing_new_withroute");
		serverParams.put("type", "listing");
		
		//初始化两个基于Elasticsearch的搜索引擎，一个是基础款，一个是相关性改善、基于路由的
		ElasticSearchEngineBasic eseb = new ElasticSearchEngineBasic(serverParams);
		ElasticSearchEngineRelevantWithRoute eserr = new ElasticSearchEngineRelevantWithRoute(eseb);
		
		// 测试基于路由的索引
//		eserr.indexListing("/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm"
//				+ "/listing-segmented-shuffled-noheader.txt", serverParams);
		
		// 测试查询接口
		Map<String, Object> queryParams = new HashMap<>();
		// 和Solr有所不同，需要在这里指定索引和类型
		queryParams.put("index", "listing_new_withroute");
		queryParams.put("type", "listing");
		// 查询关键词
		queryParams.put("query", "苹果");
		queryParams.put("fields", new String[] {"listing_title"});
		queryParams.put("from", 0);	// 从第1条结果记录开始
		queryParams.put("size", 5);	// 返回5条结果记录
		queryParams.put("mode", "BoolQuery");	// 选择基础查询模式
		
		// 对比相关性改善前后
		System.out.println("基础搜索：\t\t" + eseb.query(queryParams));	// 查询并输出
		System.out.println("相关性改良、基于路由的搜索：\t" + eserr.query(queryParams));	// 再次查询并输出
		
		eseb.cleanup();
		eserr.cleanup();
		
	}

}
