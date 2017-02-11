package SearchEngine.SearchEngineImplementation.Solr;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEnginePersonalizedInterface;
import SearchEngine.UserProfile.UserProfile;

public class SolrSearchEnginePersonalized implements SearchEnginePersonalizedInterface{
	
	private SolrSearchEngineRelevant sser = null;
	
	public SolrSearchEnginePersonalized(SolrSearchEngineRelevant sser) {
		
		// 相关性搜索引擎不变
		this.sser = sser;
		
		// 增加了用户画像获取模块
		UserProfile.init();
		
	}
	
	public void cleanup() {
		sser.cleanup();
		UserProfile.cleanup();	// 增加了用户画像的资源回收
	}
	
	// 索引部分保持不变
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		return sser.index(documents, indexParams);
		
	}
	
	

	// 查询部分附加上个性化的逻辑
	public String query(Map<String, Object> queryParams, String userid) {
		// TODO Auto-generated method stub

		QueryResponse  response = null;
		
		try {
			
			// 适配部分：根据输入的搜索请求，生成Solr所能识别的查询
			String query = queryParams.get("query").toString();
			String[] terms = query.split("\\s+");
			StringBuffer sbQuery = new StringBuffer();
			for (String term : terms) {
				if (sbQuery.length() == 0) {
					sbQuery.append(term);
				} else {
					// 为了确保相关性，使用了AND的布尔操作
					sbQuery.append(" AND ").append(term);
				}
			}
			String[] fields = (String []) queryParams.get("fields");
			StringBuffer sbQf = new StringBuffer();
			for (String field : fields) {
				if (sbQf.length() == 0) {
					sbQf.append(field);
				} else {
					// 在多个字段上查询
					sbQf.append(" ").append(field);
				}
			}
			
			// 为支持翻页（pagination）操作的起始位置和返回结果数
			int start = (int)(queryParams.get("start"));
			int rows = (int)(queryParams.get("rows"));
			
			// 构建Solr使用的查询
			SolrQuery sq = new SolrQuery();
			sq.setParam("defType", "edismax");
			sq.set("q", "*:*");
			sq.set("fq", sbQuery.toString());	// 使用过滤查询
			sq.set("qf", sbQf.toString());
			sq.set("start", start);
			sq.set("rows", rows);
			
			
			// 新增的装饰部分：根据用户ID userid，获取该用户的喜好数据
			HashMap<String, String> preference = 
					(HashMap<String, String>) UserProfile.getPrefence("user_profile", userid);
			
			// 对于分类的喜好程度，通过category_name字段的boost实现
			if (preference.containsKey("categories")) {
				String[] categories = preference.get("categories").split("[，|,]");
				for (String category : categories) {
					sq.add("bq", String.format("category_name:(%s^%f)", category, 0.1));
												// 可以根据个性化的程度、应用的需要来调整这里0.1的得分
				}
			}
			
			// 由于数据有限，目前关于品牌的喜好是通过商品标题字段listing_title实现的
			if (preference.containsKey("brands")) {
				String[] brands = preference.get("brands").split("[，|,]");
				for (String brand : brands) {
					sq.add("bq", String.format("listing_title:(%s^%f)", brand, 0.1));
												// 可以根据个性化的程度、应用的需要来调整这里0.1的得分
				}
			}
			
			// 由于数据有限，目前关于标签也是通过商品标题字段listing_title来实现的
			if (preference.containsKey("tags")) {
				String[] tags = preference.get("tags").split("[，|,]");
				for (String tag : tags) {
					sq.add("bq", String.format("listing_title:(%s^%f)", tag, 0.1));
												// 可以根据个性化的程度、应用的需要来调整这里0.1的得分
				}
			}
			
			
			
			// 之前的装饰部分：通过查询分类的结果，优化相关性。相关性仍然是最基本的，要保持较高的boost分值
			// 如下这行可以使用RESTful API或者服务化模块代替，这样模块间耦合度更低
			HashMap<String, Double> queryClassificationResults 
				= (HashMap<String, Double>) sser.nbqcsearch.predict(query);
			
			for (String cate : queryClassificationResults.keySet()) {
				double score = queryClassificationResults.get(cate);
				if (score < 0.02) continue;		// 去除得分过低的噪音点
				
				sq.add("bq", String.format("category_name:(%s^%f)", cate, score));
//				System.out.println(String.format("category_name:(%s^%f)", cate, score));
			}
			
			// 获取查询结果
			response = sser.sseb.solrClient.query(sq);
//			SolrDocumentList list = response.getResults();
//			。。。这里略去后续统一文档拼装的实现。。。
			
			
		} catch (Exception ex) {
			ex.printStackTrace();
		} 
		
		return response.toString();
	}
	
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		// 测试查询接口
		// 连接Solr Cloud的Zookeeper设置，根据你的需要设置
		Map<String, Object> serverParams = new HashMap<>();
		serverParams.put("zkHost",  "192.168.1.48:9983");
		// 查询读取哪个Collection，根据你的需要设置。
		serverParams.put("collection",  "listing_collection");
		
		// 创建基本款搜索引擎
		SolrSearchEngineBasic sseb = new SolrSearchEngineBasic(serverParams);
		// 创建相关性改良后的搜索引擎
		SolrSearchEngineRelevant sser = new SolrSearchEngineRelevant(sseb);
		// 使用装饰器模式设计的个性化搜索引擎
		SolrSearchEnginePersonalized ssep 
			= new SolrSearchEnginePersonalized(sser);
		
		Map<String, Object> queryParams = new HashMap<>();
		
		// 查询关键词
		queryParams.put("fields", 
				new String[] {"listing_title"});
		queryParams.put("start", 0);	// 从第1条结果记录开始
		queryParams.put("rows", 10);		// 返回10条结果记录
		
		
		// 对比基础搜索、相关性改善后的搜索、以及个性化搜索
		String[] queries = {"牛奶", "手机", "康师傅", "苹果"};
		LinkedHashMap<String, String> users = new LinkedHashMap<>();
		users.put("user1", "张三");
		users.put("user2", "李四");
		users.put("user3", "王五");
		users.put("user4", "赵六");
		for (String query : queries) {
			queryParams.put("query", query);
			System.out.println("基础搜索：\t\t" + sseb.query(queryParams));
			System.out.println("相关性改良后的搜索：\t" + sser.query(queryParams));
			for (String userid : users.keySet()) {
				System.out.println(String.format("%s用户个性化的搜索：\t%s", users.get(userid), ssep.query(queryParams, userid)));
			}
		}
		
		ssep.cleanup();
		
	}

}
