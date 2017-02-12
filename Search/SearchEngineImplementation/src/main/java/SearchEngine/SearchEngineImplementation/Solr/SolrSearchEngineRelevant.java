package SearchEngine.SearchEngineImplementation.Solr;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.solr.client.solrj.SolrQuery;
import org.apache.solr.client.solrj.response.QueryResponse;

import MahoutMachineLearning.Classification.NBQueryClassifierOnlineForSearch;
import SearchEngine.SearchEngineImplementation.ListingDocument;
import SearchEngine.SearchEngineImplementation.SearchEngineBasicInterface;

public class SolrSearchEngineRelevant implements SearchEngineBasicInterface{
	
	protected SolrSearchEngineBasic sseb = null;
	protected NBQueryClassifierOnlineForSearch nbqcsearch = null;
	
	public SolrSearchEngineRelevant(SolrSearchEngineBasic sseb) {
		
		// 基本的搜索引擎不变
		this.sseb = sseb;
		
		// 增加了查询分类模块
		nbqcsearch = new NBQueryClassifierOnlineForSearch();
		
	}
	
	public void cleanup() {
		sseb.cleanup();
		nbqcsearch.cleanup();	// 增加了查询分类的资源回收
	}
	
	// 索引部分保持不变
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams) {
		
		return sseb.index(documents, indexParams);
		
	}
	
	

	// 查询部分附加上相关性的逻辑
	public String query(Map<String, Object> queryParams) {
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
			
			// 新增的装饰部分：通过查询分类的结果，优化相关性
			// 如下这行可以使用RESTful API或者服务化模块代替，这样模块间耦合度更低
			HashMap<String, Double> queryClassificationResults 
				= (HashMap<String, Double>) nbqcsearch.predict(query.replaceAll("\\s+", ""));
			
			for (String cate : queryClassificationResults.keySet()) {
				double score = queryClassificationResults.get(cate);
				if (score < 0.02) continue;		// 去除得分过低的噪音点
				
				sq.add("bq", String.format("category_name:(%s^%f)", cate, score));
//				System.out.println(String.format("category_name:(%s^%f)", cate, score));
			}
			
			// 获取查询结果
			response = sseb.solrClient.query(sq);
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
		// 使用装饰器模式设计的新搜索引擎
		SolrSearchEngineRelevant sser 
			= new SolrSearchEngineRelevant(sseb);
		
		Map<String, Object> queryParams = new HashMap<>();
		// 查询关键词
		queryParams.put("query", "巧克力 牛奶");
		queryParams.put("fields", 
				new String[] {"listing_title"});
		queryParams.put("start", 0);	// 从第1条结果记录开始
		queryParams.put("rows", 5);		// 返回5条结果记录
		
		
		// 对比相关性改善前后
		System.out.println("基础搜索：\t\t" + sseb.query(queryParams));
		System.out.println("相关性改良后的搜索：\t" + sser.query(queryParams));
		
		sseb.cleanup();
		sser.cleanup();
		
	}

}
