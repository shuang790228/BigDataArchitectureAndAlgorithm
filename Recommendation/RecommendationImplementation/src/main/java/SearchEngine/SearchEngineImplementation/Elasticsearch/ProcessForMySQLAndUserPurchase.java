package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;

import org.elasticsearch.common.xcontent.XContentParser.Token;

public class ProcessForMySQLAndUserPurchase {
	
	
	public void process(String sqlConnectionUrl, String purchaseFileName, String outputFileName) {
		
		// 加载用户购买的记录，并转为商品到用户的记录
		Map<Long, String> listing2users = new HashMap<>();
		try {
			
			BufferedReader br = new BufferedReader(new FileReader(purchaseFileName));
			String strLine = br.readLine();  // 跳过header行
			while ((strLine = br.readLine()) != null) {
				String[] tokens = strLine.split("\t");
				if (tokens.length < 2) continue;
				
				String userId = tokens[0];
				String[] listingIds = tokens[1].split(" ");
				
				for (String listingId : listingIds) {
					Long llistingId = Long.parseLong(listingId);
					if (listing2users.containsKey(llistingId)) {
						listing2users.put(llistingId, 
								String.format("%s %s", listing2users.get(llistingId), userId));
					} else {
						listing2users.put(llistingId, userId);
					}
				}
				
			}
			
			br.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		}
		
		
		
		Connection conn = null;
		PrintWriter pw = null;
		
		try {
	        
            // 使用MySQL的驱动器，需要在pom.xml中指定依赖的mysql包
            com.mysql.jdbc.Driver driver = new com.mysql.jdbc.Driver();
            // 一个Connection进行一个数据库连接
            conn = DriverManager.getConnection(sqlConnectionUrl);
            // Statement里面带有很多方法，比如executeUpdate可以实现插入，更新和删除等
            Statement stmt = conn.createStatement();
            
            // 保存输出的文件
            pw = new PrintWriter(new FileWriter(outputFileName));
            
            int batch = 1000;	// 每次读取1000条记录并写入输出文件
            int start = 0;
            String jsonLine1 = "{ \"index\" : { \"_index\" : \"listing_vs_user\", \"_type\" : \"listing\" } }";
            
            
            while (true) {
	            
            	String sql = String.format("SELECT * FROM listing_segmented_shuffled limit %d, %d",
	            		start, batch);
	            ResultSet rs = stmt.executeQuery(sql);	// executeQuery语句会返回SQL查询的结果集
		        
	            int returnCnt = 0;
	            while (rs.next()) {
	            	
	            	// 读取记录并拼装JSON对象
	            	long listing_id = rs.getLong("listing_id");
	            	String listing_title = rs.getString("listing_title");
	            	long category_id = rs.getLong("category_id");
	            	String category_name = rs.getString("category_name");
	            	// 下述这行是新增的，用于写入购买了该商品的用户列表
	            	String users = listing2users.get(listing_id);
	            	
	            	String jsonLine2 = String.format("{ \"listing_id\" : \"%d\", \"listing_title\" : \"%s\", "
	            			+ "\"category_id\" : \"%d\", \"category_name\" : \"%s\", "
	            			+ "\"purchased_users\" : \"%s\" }",
	            			listing_id, listing_title, 
	            			category_id, category_name, 
	            			users);
	            	
	            	// 将JSON对象写入输出文件
	            	pw.println(jsonLine1);
	            	pw.println(jsonLine2);
	            	
	            	returnCnt ++;
	            }
	            
	            if (returnCnt < batch) break;		// 没有更多的查询结果了，退出
	            start += batch;		// 查询下一1000条记录
            }
		
			
			pw.close();
			conn.close();
			
		} catch (Exception e) {
			// TODO: handle exception
			e.printStackTrace();
		} finally {		//最后的扫尾工作
			if (pw != null) pw.close();
			if (conn != null)
				try {
					conn.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
		}
		
	}
	

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		ProcessForMySQLAndUserPurchase pfmysql = new ProcessForMySQLAndUserPurchase();
		// MySQL JDBC URL：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
        String sqlConnectionUrl = "jdbc:mysql://localhost:3306/sys?user=root&password=830728&useUnicode=true&characterEncoding=UTF8";
        pfmysql.process(sqlConnectionUrl, 
        		"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/user-purchases.txt",
        		"/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-userpurchases-for-elasticsearch.txt");
		

	}

}
