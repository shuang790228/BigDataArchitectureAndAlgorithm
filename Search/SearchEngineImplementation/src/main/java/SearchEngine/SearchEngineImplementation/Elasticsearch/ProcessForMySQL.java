package SearchEngine.SearchEngineImplementation.Elasticsearch;

import java.io.FileWriter;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class ProcessForMySQL {
	
	
	public void process(String sqlConnectionUrl, String outputFileName) {
		
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
            String jsonLine1 = "{ \"index\" : { \"_index\" : \"listing_new\", \"_type\" : \"listing\" } }";
            
            
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
	            	
	            	String jsonLine2 = String.format("{ \"listing_id\" : \"%d\", \"listing_title\" : \"%s\", \"category_id\" : \"%d\", \"category_name\" : \"%s\" }",
	            			listing_id, listing_title, category_id, category_name);
	            	
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
		
		ProcessForMySQL pfmysql = new ProcessForMySQL();
		// MySQL JDBC URL：jdbc:mysql://主机名称：连接端口/数据库的名称?参数=值
        String sqlConnectionUrl = "jdbc:mysql://localhost:3306/sys?user=root&password=yourownpassword&useUnicode=true&characterEncoding=UTF8";
        pfmysql.process(sqlConnectionUrl, "/Users/huangsean/Coding/data/BigDataArchitectureAndAlgorithm/listing-segmented-shuffled-for-elasticsearch.txt");
		

	}

}
