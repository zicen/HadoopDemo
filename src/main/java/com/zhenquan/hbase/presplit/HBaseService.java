package com.zhenquan.hbase.presplit;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.zhenquan.hbase.utils.DateUtils;
import com.zhenquan.hbase.utils.JDBCUtil;
import com.zhenquan.hbase.utils.MyStringUtil;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FamilyFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.QualifierFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.filter.ValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.MD5Hash;


/**
 * hbase 实现接口
 * 
 * @author dajiangtai
 * 
 */
public class HBaseService {
	public static HBaseClient hbaseClient;

	public HBaseService() {
		hbaseClient = HBaseClient.getHBaseClient();
	}

	public static void main(String[] args) throws IOException,
			InterruptedException {
		HBaseService hBaseService = new HBaseService();
//		hBaseService.insertTask();
		// 根据Rowkey查询
		 String row =
		 MD5Hash.getMD5AsHex(Bytes.toBytes("0011706")).substring(0,
		 8)+"0011706"+"20160616"+"33005";
		 hBaseService.getTaskByRowkey(row);

		// 精确到用户uid查询
//		 int uid = 11772;
//		 hBaseService.getTaskByUid(uid);

		// 精确到日期查询
//		 int uid = 11772;
//		 String time = "2016-07-04";
//		 hBaseService.getTaskByUidAndTime(uid, time);

		// 根据RowFilter查询
//		 hBaseService.getTaskByRowFilter();

		//根据FamilyFilter过滤
//		String columnFamily="cf";
//		//String columnFamily="info";
//		hBaseService.getTaskByFamilyFilter(columnFamily);
		
		// 根据QualifierFilter过滤
//		 int uid = 11772;
//		 String column = "name";
//		 hBaseService.getTaskByQualifierFilter(uid, column);

		// 根据ValueFilter过滤***
//		 String value = "3204";
//		 hBaseService.getTaskByValueFilter(value);

		// 根据SingleColumnValueFilter过滤
//		 int uid = 11772;
//		 hBaseService.getTaskBySingleColumnValueFilter(uid);

		// 根据PrefixFilter过滤 0011772:20160630:34445
//		 int row = 11772;
//		 hBaseService.getTaskByPrefixFilter(row);

		// 计数器使用 创建表 create 'statistics','count'
//		 String row = "djt";
//		 String family = "count";
//		 String column = "pv";
//		 long value = 1;
//		 int num = 0;
//		 for(int i=0;i<1000;i++){
//		 hBaseService.updateByIncrement(row, family, column, value);
//		 num++;
//		 System.out.println(num);
//		 }
//		 //hbaseClient太重，不能放到循环内
//		 hbaseClient.close();

		// 查询pv
//		 String tableName = "tesk";
//		 String row = "cf";
//		 hBaseService.getStatisticsByRowkey(tableName, row);
	}

	/**
	 * 根据Rowkey查询 Statistics
	 * 
	 * @param row
	 */
	public void getStatisticsByRowkey(String tableName, String row) {
		// 获取表句柄
		Table table = hbaseClient.getTable(tableName);
		Get get = new Get(row.getBytes());
		try {
			Result result = table.get(get);
			if (!result.isEmpty()) {
				long pv = Bytes.toLong(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("count"),
								Bytes.toBytes("pv"))));
				System.out.println(pv);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 计数器使用 create 'statistics','count'
	 * 
	 * @param row
	 * @param family
	 * @param column
	 * @param value
	 */
	public void updateByIncrement(String row, String family, String column,
			long value) {
		// 获取表句柄hardcode
		Table table = hbaseClient.getTable("statistics");
		try {
			table.incrementColumnValue(Bytes.toBytes(row),
					Bytes.toBytes(family), Bytes.toBytes(column), value);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (table != null) {
				hbaseClient.closeTable(table);
			}
		}
	}

	/**
	 * 根据PrefixFilter过滤 筛选出具有特定前缀的行键的数据。
	 * 
	 * @param
	 */
	public void getTaskByPrefixFilter(int uid) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);

		String fixedUid = MyStringUtil.getFixedLengthStr(uid + "", 7);
		String row = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid)).substring(0,
				8)
				+ fixedUid;
		PrefixFilter prefixFilter = new PrefixFilter(Bytes.toBytes(row));
		Scan s = new Scan();

		s.setFilter(prefixFilter);
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());

				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				System.out.println(rowkey + ":" + name);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 根据SingleColumnValueFilter过滤
	 * 
	 * @param uid
	 */
	public void getTaskBySingleColumnValueFilter(int uid) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);

		String fixedUid1 = MyStringUtil.getFixedLengthStr(uid + "", 7);
		String startKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid1))
				.substring(0, 8) + fixedUid1;

		String fixedUid2 = MyStringUtil.getFixedLengthStr(uid + 1 + "", 7);
		String endKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid1))
				.substring(0, 8) + fixedUid2;

		// 单值过滤
		// MUST_PASS_ONE只要scan的数据行符合其中一个filter就可以返回结果(但是必须扫描所有的filter)
		// MUST_PASS_ALL必须所有的filter匹配通过才能返回数据行
		// (但是只要有一个filter匹配没通过就算失败，后续的filter停止匹配)
		FilterList filterList = new FilterList(
				FilterList.Operator.MUST_PASS_ONE);
		// value = 3,5
		filterList.addFilter(new SingleColumnValueFilter(Bytes.toBytes("cf"),
				Bytes.toBytes("type"), CompareOp.EQUAL, Bytes.toBytes("9")));
		// .....

		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(startKey));
		s.setStopRow(Bytes.toBytes(endKey));

		s.setFilter(filterList);
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());

				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				String type = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("type"))));
				System.out.println(rowkey + ":" + name + "\t" + type);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 根据ValueFilter过滤 按照具体的值来筛选单元格的过滤器，这会把一行中值不能满足的单元格过滤掉，
	 * 如下面的构造器，对于每一行的一个列，如果其对应的值不包含 那么这个列就不会返回给客户端
	 * 
	 * @param value
	 */
	public void getTaskByValueFilter(String value) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);

		// 值过滤
		BinaryComparator comp = new BinaryComparator(Bytes.toBytes(value)); //
		ValueFilter filter = new ValueFilter(CompareOp.EQUAL, comp);
		Scan s = new Scan();
		s.setFilter(filter);
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());
				// 其他列如果不匹配value，就不会返回，比如name字段被过滤掉
				String systaskid = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("systaskid"))));
				System.out.println(rowkey + ":" + systaskid);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 根据QualifierFilter过滤
	 * 
	 * @param uid
	 * @param column
	 */
	public void getTaskByQualifierFilter(int uid, String column) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);

		// 列过滤
		QualifierFilter qualifierFilter = new QualifierFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(column)));

		String fixedUid1 = MyStringUtil.getFixedLengthStr(uid + "", 7);
		String startKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid1))
				.substring(0, 8) + fixedUid1;

		String fixedUid2 = MyStringUtil.getFixedLengthStr(uid + 1 + "", 7);
		String endKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid1))
				.substring(0, 8) + fixedUid2;

		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(startKey));
		s.setStopRow(Bytes.toBytes(endKey));
		s.setFilter(qualifierFilter);
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());
				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				System.out.println(rowkey + "@" + name);
				// 经过过滤后，taskid字段不存在，此时会报错
				String taskid = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("taskid"))));
				System.out.println(rowkey + ":" + taskid);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 根据FamilyFilter过滤 可设置多个列簇进行测试
	 * 
	 * @param columnFamily
	 */
	public void getTaskByFamilyFilter(String columnFamily) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);

		// 列簇过滤
		FamilyFilter familyFilter = new FamilyFilter(CompareOp.EQUAL,
				new BinaryComparator(Bytes.toBytes(columnFamily)));
		Scan s = new Scan();
		s.setFilter(familyFilter);
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());
				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				System.out.println(rowkey + ":" + name);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 根据RowFilter查询
	 * 
	 */
	public void getTaskByRowFilter() {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);

		// 根据Rowkey过滤
		// RowFilter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,new
		// BinaryComparator(Bytes.toBytes(rowKeyValue)));
		// 提取rowkey以包含20160630的数据
		RowFilter rowFilter = new RowFilter(CompareOp.EQUAL,
				new SubstringComparator("20160704"));
		Scan s = new Scan();
		s.setFilter(rowFilter);
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());
				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				System.out.println(rowkey + ":" + name);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * scan 精确到time
	 * 
	 * @param uid
	 * @param time
	 */
	public void getTaskByUidAndTime(int uid, String time) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);
		String fixedUid = MyStringUtil.getFixedLengthStr(uid + "", 7);

		String startKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid))
				.substring(0, 8)
				+ fixedUid
				+ DateUtils.getDateFormatFromDay(DateUtils.YMD, time,
						DateUtils.YYYYMMDD);

		// 天数加1
		String endKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid)).substring(
				0, 8)
				+ fixedUid
				+ DateUtils.getDateBeforeOrAfter(DateUtils.YMD, time, 1,
						DateUtils.YYYYMMDD);
		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(startKey));
		s.setStopRow(Bytes.toBytes(endKey));
		ResultScanner rsa = null;
		ArrayList<Task> list = new ArrayList<Task>();
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				Task task = new Task();
				String rowkey = new String(result.getRow());
				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				System.out.println(rowkey + ":" + name);
				task.setName(name);
				list.add(task);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * scan 精确到uid
	 * 
	 * @param uid
	 */
	public void getTaskByUid(int uid) {
		// 获取表句柄101 102
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);
		String fixedUid1 = MyStringUtil.getFixedLengthStr(uid + "", 7);
		String startKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid1))
				.substring(0, 8) + fixedUid1;

		String fixedUid2 = MyStringUtil.getFixedLengthStr(uid + 1 + "", 7);
		String endKey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid1))
				.substring(0, 8) + fixedUid2;

		Scan s = new Scan();
		s.setStartRow(Bytes.toBytes(startKey));
		s.setStopRow(Bytes.toBytes(endKey));
		ResultScanner rsa = null;
		try {
			rsa = table.getScanner(s);
			for (Result result : rsa) {
				String rowkey = new String(result.getRow());
				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				System.out.println(rowkey + ":" + name);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (rsa != null) {
				rsa.close();
			}
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 根据Rowkey查询 scan 'task',{LIMIT=>5}
	 * 
	 * @param row
	 */
	public void getTaskByRowkey(String row) {
		// 获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);
		Get get = new Get(row.getBytes());
		try {
			Result result = table.get(get);
			if (!result.isEmpty()) {
				String name = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("name"))));
				String type = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("type"))));
				String taskid = Bytes.toString(CellUtil.cloneValue(result
						.getColumnLatestCell(Bytes.toBytes("cf"),
								Bytes.toBytes("taskid"))));
                String receivedate = Bytes.toString(CellUtil.cloneValue(result.getColumnLatestCell(Bytes.toBytes("cf"),
                        Bytes.toBytes("receivedate"))));
				System.out.println(name + "@" + type + "@" + taskid+"@"+receivedate);
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}

	/**
	 * 插入所有测试数据数据。   就是将之前存在mysql上面的数据放到hbase上面，然后我们再根据不同的情况去查询这些数据。
     * 在这里面列簇只有一个，剩下的全是列，列就是原来mysql里面的字段，值就是原来mysql每行的值
	 */
	public void insertTask() {
		String sql = "select * from task";
		//1. 从mysql提取测试数据
		List<Task> list = JDBCUtil.queryData(sql);
		// 2.获取表句柄
		Table table = hbaseClient.getTable(hbaseClient.TABLE_NAME);
		List<Put> putList = new ArrayList<Put>();
		//3.遍历构建出Put
		for (Task task : list) {
			// 拼接Rowkey uid + time +taskid 1 101
			String fixedUid = MyStringUtil.getFixedLengthStr(
					task.getUid() + "", 7);

			String rowkey = MD5Hash.getMD5AsHex(Bytes.toBytes(fixedUid))
					.substring(0, 8)
					+ fixedUid
					+ DateUtils.getDateFormatFromDay(DateUtils.YMD_HMS,
							task.getStarttime(), DateUtils.YYYYMMDD)
					+ task.getTaskid();

			Put put = new Put(Bytes.toBytes(rowkey));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_ACTUALFINISHTIME),
					Bytes.toBytes(task.getActualfinishtime()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_FINISHTIME),
					Bytes.toBytes(task.getFinishtime()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_NAME),
					Bytes.toBytes(task.getName()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_RECEIVETIME),
					Bytes.toBytes(task.getReceivedate()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_STARTTIME),
					Bytes.toBytes(task.getStarttime()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_STATE),
					Bytes.toBytes(task.getState()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_SYSTASKID),
					Bytes.toBytes(task.getSystaskid()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_TASKID),
					Bytes.toBytes(task.getTaskid()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_TYPE),
					Bytes.toBytes(task.getType()));
			put.addColumn(Bytes.toBytes(hbaseClient.COLUMNFAMILY),
					Bytes.toBytes(hbaseClient.COLUMNFAMILY_UID),
					Bytes.toBytes(task.getUid()));
			putList.add(put);
		}
		try {
		    //4.插入
			table.put(putList);
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} finally {
			if (table != null) {
				hbaseClient.closeTable(table);
			}
			hbaseClient.close();
		}
	}
}
