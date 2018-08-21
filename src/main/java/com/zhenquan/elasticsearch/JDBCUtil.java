package com.zhenquan.elasticsearch;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
/**
 * mysql 工具类
 * @author 大讲台
 *
 */
public class JDBCUtil {

	//static Connection conn = null;
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
	 * 插入数据
	 * @param actor
	 * @param director
	 * @param allnumber
	 * @param tvtype
	 * @param tvname
	 * @param description
	 * @param tvid
	 * @param alias
	 * @param tvshow
	 * @param present
	 * @param score
	 * @param zone
	 * @param commentnumber
	 * @param supportnumber
	 * @param pic
	 */
	public static void insertData(String actor,String director,String allnumber,String tvtype,
			String tvname,String description,String tvid,String alias,String tvshow,
			String present,String score,String zone,String commentnumber,String supportnumber,String pic){
		String sql = "insert into tvcount(tvname,director,actor,allnumber,tvtype,description,tvid,alias,tvshow,present,score,zone,commentnumber,supportnumber,pic) values ('"+
			tvname+"','"+director+"','"+actor+"','"+allnumber+"','"+tvtype+"','"+description+"','"+tvid
			+"','"+alias+"','"+tvshow+"','"+present+"','"+score+"','"+zone+"','"+commentnumber
			+"','"+supportnumber+"','"+pic+"')";
		System.out.println(sql);
		Connection conn = getConnectionByJDBC();
		Statement stmt;
		try {
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			conn.commit();
			
			stmt.close();
			conn.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	
		
	}
	/**
	 * 查询数据
	 * @param sql
	 * @return
	 */
	public static List<TVCount> queryData(String sql){
		Connection conn = JDBCUtil.getConnectionByJDBC();
		Statement stmt;
		ArrayList<TVCount> arrayList = new ArrayList<TVCount>();
		try {
			stmt= conn.createStatement();
			ResultSet set = stmt.executeQuery(sql);
			
			while(set.next()){
				TVCount tv = new TVCount();
				tv.setTvname(set.getString(1));
				tv.setDirector(set.getString(2));
				tv.setActor(set.getString(3));
				tv.setAllnumber(set.getString(4));
				tv.setTvtype(set.getString(5));
				tv.setDescription(set.getString(6));
				tv.setTvid(set.getString(7));
				 tv.setAlias(set.getString(8));
				 tv.setTvshow(set.getString(9));
				 tv.setPresent(set.getString(10));
				 tv.setScore(set.getString(11));
				 tv.setZone(set.getString(12));
				 tv.setCommentnumber(set.getString(13));
				 tv.setSupportnumber(set.getString(14));
				 tv.setPic(set.getString(15));
				 arrayList.add(tv);
			}			
			stmt.close();
			conn.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return arrayList;
	}
	
	public static void main(String[] args){
	}
	
	
}
