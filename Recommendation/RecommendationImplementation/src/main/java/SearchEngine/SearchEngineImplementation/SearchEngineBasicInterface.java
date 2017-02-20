package SearchEngine.SearchEngineImplementation;

import java.util.List;
import java.util.Map;

public interface SearchEngineBasicInterface {
	
	// 这里只实现了索引和查询接口，感兴趣的读者可以自行尝试聚集等其他操作的接口
	public String index(List<ListingDocument> documents, Map<String, Object> indexParams);
	public String query(Map<String, Object> queryParams);
	
//	public String aggregate(...);
	

}
