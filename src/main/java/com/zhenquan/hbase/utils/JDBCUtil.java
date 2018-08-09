package com.zhenquan.hbase.utils;
import com.zhenquan.hbase.Task;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
/**
 * mysql 工具类
 * @author dajiangtai
 *
 */
public class JDBCUtil {
	/**
	 * 连接数据库
	 * @return
	 */
	public static Connection getConnectionByJDBC() {
		Connection conn = null;
		if(conn == null){
			try{
				Class.forName(Constant.MYSQL_DRIVER);
				conn = DriverManager.getConnection(Constant.MYSQL_URL,Constant.MYSQL_USERNAME,Constant.MYSQL_PASSWORD);
				conn.setAutoCommit(false);
			}catch(Exception e) {
				e.printStackTrace();
			}
		}
		return conn;
	}
	
	
	/**
	 * 查询数据
	 * @param
	 * @return
	 */
	@Test
	public  static List<Task> queryData(String sql){
//		String sql = "select * from task";
		Connection conn = JDBCUtil.getConnectionByJDBC();
		Statement stmt;
		ArrayList<Task> arrayList = new ArrayList<Task>();
		try {
			stmt= conn.createStatement();
			ResultSet set = stmt.executeQuery(sql);
			while(set.next()){
				Task task = new Task();
				task.setUid(isNull(set.getString("uid")));
				task.setTaskid(isNull(set.getString("taskid")));
				task.setSystaskid(isNull(set.getString("systaskid")));
				task.setName(isNull(set.getString("name")));
				task.setType(isNull(set.getString("type")));
				task.setStarttime(isNull(set.getString("starttime")));
				task.setFinishtime(isNull(set.getString("finishtime")));
				task.setReceivedate(isNull(set.getString("receivedate")));
				task.setActualfinishtime(isNull(set.getString("actualfinishtime")));
				task.setState(isNull(set.getString("state")));
				System.out.println(task.getUid()+":"+task.getSystaskid()+":"+task.getName()
						+":"+task.getType()+":"+task.getStarttime()+":"
						+task.getFinishtime()+":"+task.getReceivedate()+":"
						+task.getActualfinishtime()+":"+task.getState());
				arrayList.add(task);
			}			
			stmt.close();
			conn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return arrayList;
	}
	/**
	 * 统计总记录数
	 * @return
	 */
	public static int getAllRecords(String sql){
		Connection conn = JDBCUtil.getConnectionByJDBC();
		Statement stmt;
		int count = 0;
		try {
			stmt= conn.createStatement();
			ResultSet set = stmt.executeQuery(sql);
			set.next();
			count = set.getInt(1);
			System.out.println(count);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
			return count;
	}
	/**
	 * 转换null字符串
	 * @param str
	 * @return
	 */
	public static String isNull(String str){
		if(str == null){
			str = "";
		}
		return str;
	}
	
	public static void main(String[] args){
		String sql = "select count(*) from task_hbase";
		JDBCUtil.getAllRecords(sql);
	}
	
	
}
