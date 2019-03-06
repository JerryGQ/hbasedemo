package com.shujia.hbase;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;

import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;


public class HBaseDemo {

	String TN = "phone";

	HBaseAdmin hbaseAdmin;

	HTable htable;

	@Before
	public void begin() throws Exception {
		//创建配置信息类
		Configuration conf = new Configuration();
		//配置zk地址列表
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		//创建hbase管理类
		hbaseAdmin = new HBaseAdmin(conf);
		//创建hbase table类
		htable = new HTable(conf, TN);
	}

	@After
	public void end() throws Exception {
		if (hbaseAdmin != null) {
			hbaseAdmin.close();
		}
		if (htable != null) {
			htable.close();
		}
	}

	/**
	 * 创建表
	 * @throws Exception
	 */
	@Test
	public void createTable() throws Exception {

		//判断表是否存在
		if (hbaseAdmin.tableExists(TN)) {
			//使表失效
			hbaseAdmin.disableTable(TN);
			//删除表
			hbaseAdmin.deleteTable(TN);
		}
		//创建hbase表描述
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		//创建列族描述
		HColumnDescriptor cf = new HColumnDescriptor("cf1");
		//设置缓存
		cf.setInMemory(true);
		//设置版本号
		cf.setMaxVersions(2);
		//增加列族描述
		desc.addFamily(cf);

		hbaseAdmin.createTable(desc);
	}


	/**
	 * 插入数据
	 * @throws Exception
	 */
	@Test
	public void insertDB() throws Exception {
		String phoneNum = getPhone("186");
        String time = getDate("2018");
		String rowkey = phoneNum + "_" + time;
		//创建put数据的类
		Put put = new Put(rowkey.getBytes());
		//存手机号
		put.add("cf1".getBytes(), "phoneNum".getBytes(), phoneNum.getBytes());
        //时间
		put.add("cf1".getBytes(), "time".getBytes(), time.getBytes());
		//对端手机号
		put.add("cf1".getBytes(), "dpNum".getBytes(), getPhone("177")
				.getBytes());
		//类型
		put.add("cf1".getBytes(), "type".getBytes(),
				Bytes.toBytes(r.nextInt(2)));
		htable.put(put);
	}

	@Test
	public void get() throws Exception {

	    //创建gei类，查询数据，穿一个ROWKey
		Get get = new Get("18674464161_20180728212417".getBytes());
		//指定查询的列
		get.addColumn("cf1".getBytes(), "type".getBytes());
		//执行查询操作
        Result rs = htable.get(get);
        //获取一个单元格
		Cell cell = rs.getColumnLatestCell("cf1".getBytes(),
				"type".getBytes());
		//转换成字符串
		System.out.println(new String(CellUtil.cloneValue(cell)));
	}

	/**
	 * 插入批量数据 十个用户手机号 每个用户对应 产生一百条通话记录
	 */
	@Test
	public void insertDBs() throws Exception {
		List<Put> puts = new ArrayList<Put>();

		for (int i = 0; i < 10; i++) {
			String phoneNum = getPhone("186");

			for (int j = 0; j < 100; j++) {
				String time = getDate("2018");

				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

				// rowkey 查询 需要通过时间做降序排序 大数-小数
				String rowkey = phoneNum + "_"
						+ (Long.MAX_VALUE - sdf.parse(time).getTime());

				Put put = new Put(rowkey.getBytes());
				put.add("cf1".getBytes(), "phoneNum".getBytes(),
						phoneNum.getBytes());
				put.add("cf1".getBytes(), "time".getBytes(), time.getBytes());
				put.add("cf1".getBytes(), "dpNum".getBytes(), getPhone("170")
						.getBytes());
				put.add("cf1".getBytes(), "type".getBytes(),
						(r.nextInt(2) + "").getBytes());
				puts.add(put);
			}
		}
		htable.put(puts);
	}

	/**
	 * 范围查找 18686329636手机号 201701月份 所有的通话记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDBs() throws Exception {
	    //创建扫描器
		Scan scan = new Scan();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
		// 范围查找 起始rowkey  sdf.parse("20180201000000").getTime()获取时间戳
		String startRowkey = "18692168813" + "_"+ (Long.MAX_VALUE - sdf.parse("20180201000000").getTime());
		// 结束rowkey
		String stopRowkey = "18692168813" + "_" + (Long.MAX_VALUE - sdf.parse("20180101000000").getTime());
		//设置开始key
		scan.setStartRow(startRowkey.getBytes());
		//设置结束key
		scan.setStopRow(stopRowkey.getBytes());
		//执行表扫描
		ResultScanner rss = htable.getScanner(scan);

		for (Result rs : rss) {
			System.out.println(new String(CellUtil.cloneValue(rs
					.getColumnLatestCell("cf1".getBytes(),
							"phoneNum".getBytes())))
					+ "  "
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell(
							"cf1".getBytes(), "time".getBytes())))
					+ "  "
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell(
							"cf1".getBytes(), "dpNum".getBytes())))
					+ "  "
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell(
							"cf1".getBytes(), "type".getBytes()))));

		}

	}

	/**
	 * 范围查找 18686329636手机号 
	 * type=1 主叫类型 所有的通话记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void scanDBs2() throws Exception {
	    //创建扫描器
		Scan scan = new Scan();
		//创建过滤器集合
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        //创建前缀过滤器
		PrefixFilter filter1 = new PrefixFilter("18692168813".getBytes());

		//增加列过滤条件
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf1".getBytes(),
				"type".getBytes(), CompareFilter.CompareOp.EQUAL, Bytes.toBytes("1"));

		list.addFilter(filter2);
		list.addFilter(filter1);
		//将过滤器集合增加在扫描器里面
		scan.setFilter(list);
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out.println(new String(CellUtil.cloneValue(rs
					.getColumnLatestCell("cf1".getBytes(),
							"phoneNum".getBytes())))
					+ "  "
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell(
							"cf1".getBytes(), "time".getBytes())))
					+ "  "
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell(
							"cf1".getBytes(), "dpNum".getBytes())))
					+ "  "
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell(
							"cf1".getBytes(), "type".getBytes()))));

		}
	}

	//随机数
	Random r = new Random();

	/**
	 * 随机生成手机号码
	 * 
	 * @param prefix
	 *            前缀 eq:186
	 * @return 手机号码 eq：18612341234
	 */
	public String getPhone(String prefix) {
		return prefix + String.format("%08d", r.nextInt(99999999));
	}

	/**
	 * 随机生成日期
	 * 
	 * @param year
	 *            年份
	 * @return 格式:yyyyMMddHHmmss eq：20170208150401
	 */
	public String getDate(String year) {
		return year
				+ String.format(
						"%02d%02d%02d%02d%02d",
				r.nextInt(12) + 1, r.nextInt(29) + 1,
				r.nextInt(24), r.nextInt(60), r.nextInt(60));
	}
	

	/**
	 * 随机生成日期
	 * 
	 * @param  pref 年份月份日期
	 * @return 格式:yyyyMMddHHmmss eq：20170208150401
	 */
	public String getDayDate(String pref) {
		return pref
				+ String.format(
						"%02d%02d%02d",
				r.nextInt(24), r.nextInt(60), r.nextInt(60));
	}

	public static void main(String[] args) {
		HBaseDemo demo = new HBaseDemo();
		System.out.println(demo.getPhone("170"));
		System.out.println(demo.getDate("2017"));
		System.out.println(demo.getDayDate("20170209"));
	}
}
