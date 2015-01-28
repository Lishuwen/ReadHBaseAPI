package com.lsw.test;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import com.lsw.other.DayList;

public class PlanBCTest {

	/**
	 * @param args
	 */
	public Configuration config;
	public HTable table;
	public HTable table2;
	public HBaseAdmin admin;

	public PlanBCTest() {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "10.10.2.41:6000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "10.10.2.41");
		try {
			table = new HTable(config, Bytes.toBytes("traffdir_5minute"));
			table2 = new HTable(config, Bytes.toBytes("traffdir_day"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public JSONArray scan(String start, String stop, int dayNum)
			throws IOException {
		// long st=System.currentTimeMillis();
		String startRow = "1" + "\001" + "2" + "\001" + "2" + "\001" + "web"
				+ "\001"+"65"+"\001" + start;
		String stopRow = "1" + "\001" + "2" + "\001" + "2" + "\001" + "web"
				+ "\001"+"65"+"\001"  + stop;
		System.out.println("*****start rowkey----->"+startRow);
		System.out.println("*****stop rowkey----->"+stopRow);
		Scan scanner = new Scan();
		scanner.setStartRow(Bytes.toBytes(startRow));
		scanner.setStopRow(Bytes.toBytes(stopRow));
		// scanner.setTimeRange(0L, Long.MAX_VALUE);
		scanner.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"));
		// scanner.setBatch(0);
		// scanner.setCaching(100000);
		// scanner.setMaxVersions();
		ResultScanner reScanner = table2.getScanner(scanner);
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i <= dayNum; i++) {
			versionMap.put(i, 0.0);
		}
		double tmpSum = 0.0;
		for (Result rs : reScanner) {
			for (KeyValue kv : rs.raw()) {
				System.out.println("rowkey----->"+kv.getRow());
				String arr[] = Bytes.toString(kv.getRow()).split("\001");
				versionMap.put(
						Integer.parseInt(arr[1]),
						tmpSum
								+ Double.parseDouble((Bytes.toString(kv
										.getValue()))));

				System.out.println("###this is the test output###");
				System.out.print(Bytes.toString(kv.getKey()) + "  ");

				System.out.println(Bytes.toString(kv.getValue()) + "  ");
				System.out.println("######this is the test output########");
			}
		}
		JSONArray jsonArray = new JSONArray();
		for (int i = 1; i <= versionMap.size(); i++) {
			jsonArray.add(versionMap.get(i));
		}
		reScanner.close();
		return jsonArray;
	}

	public JSONArray GetData(List<Date> dayList) throws IOException {
		List<Get> getList = new ArrayList<Get>();
		  SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");  
		for (Date day : dayList) {
			
			String rowkey = sdf.format(day).replace('-', '/')+"\001"+"1" + "\001" + "2" + "\001" + "2" + "\001" + "web"
					+ "\001"+"65";
			System.out.println("*****rowkey----->"+rowkey);
			Get get = new Get(Bytes.toBytes(rowkey));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"));
			getList.add(get);
		}
		Result[] rsArr = table.get(getList);
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i <= dayList.size(); i++) {
			versionMap.put(i, 0.0);
		}
		double tmpSum = 0.0;
		for (int i = 0; i < rsArr.length - 1; i++) {
			for (KeyValue kv : rsArr[i].raw()) {
				String arr[] = Bytes.toString(kv.getKey()).split("\001");
				versionMap.put(
						Integer.parseInt(arr[1]),
						tmpSum
								+ Double.parseDouble((Bytes.toString(kv
										.getValue()))));

				System.out.println("###this is the test output###");
				System.out.print(Bytes.toString(kv.getKey()) + "  ");

				System.out.println(Bytes.toString(kv.getValue()) + "  ");
				System.out.println("######this is the test output########");

			}
		}
		JSONArray jsonArray = new JSONArray();
		for (int i = 1; i <= versionMap.size(); i++) {
			jsonArray.add(versionMap.get(i));
		}
		return jsonArray;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		PlanBCTest test = new PlanBCTest();
		DayList dy = new DayList();
//		String s = args[0].replace('/', '-');
//		String e = args[1].replace('/', '-');
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		Date dBegin = null, dEnd = null;
//		try {
//			dBegin = sdf.parse(s);
//			dEnd = sdf.parse(e);
//		} catch (ParseException e1) {
//			// TODO Auto-generated catch block
//			e1.printStackTrace();
//		}
//		List<Date> lDate = dy.findDates(dBegin, dEnd);
		List<Date> lDate = dy.findDates(args[0], args[1]);
		JSONArray jsonArray = new JSONArray();
		long st = System.currentTimeMillis();
		if (args[2].equals("G")) {
			jsonArray = test.GetData(lDate);
		} else if (args[2].equals("S")) {
			jsonArray = test.scan(args[0], args[1], lDate.size());
		}
		long et = System.currentTimeMillis();
		System.out.println(et - st);
		System.out.println("********JSONArray*********");
		System.out.println(jsonArray.toString());
	}

}
