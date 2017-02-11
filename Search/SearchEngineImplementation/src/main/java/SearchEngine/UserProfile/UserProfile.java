package SearchEngine.UserProfile;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;

public class UserProfile {
	
	// 基本配置和连接 
	private static Configuration conf = null;
	private static Connection conn = null;
	private static HTable htable = null;
	
	// HBase连接的相关配置和初始化
	public static synchronized void init() {
		
		if (conf == null || conn == null) {
			conf = HBaseConfiguration.create();
			conf.set("hbase.zookeeper.property.clientPort", "2181");
			conf.set("hbase.zookeeper.quorum", "127.0.0.1:2181");
			conf.set("hbase.master", "16000");
			
			try {
				conn = ConnectionFactory.createConnection(conf);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		
	}
	
	// 释放资源
	public static void cleanup() {
		if (conn != null) {
			try {
				conn.close();
				conn = null;
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
				conn = null;
			}
		}
		
		if (conf != null) {
			conf.clear();
			conf = null;
		}
	}
	
	public static Map<String, String> getPrefence(String table, String userid) {
		
		Map<String, String> preference = new HashMap<>();
		
		// 连接指定的HBase表格
		try {
			htable = (HTable) conn.getTable(TableName.valueOf(table));
			Get get = new Get(userid.getBytes());
			
			Result res = htable.get(get);
			
			System.out.println(new String(res.getRow()));
			
			// 读取HBase表格user_profile中的各个字段，获取用户感兴趣的分类
			for (Cell cell : res.rawCells()) {
				String columnFamily = new String(CellUtil.cloneFamily(cell));
				if ("datafields".equalsIgnoreCase(columnFamily)) {
					String qualifier = new String(CellUtil.cloneQualifier(cell));
					preference.put(qualifier, new String(CellUtil.cloneValue(cell)));
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return preference;
		
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		UserProfile.init();
		
		System.out.println(UserProfile.getPrefence("user_profile", "user1"));
		
		UserProfile.cleanup();

	}

}
