package com.lsw.day0916;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadHBaseIllegalRouteTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table;
	private HBaseAdmin admin;

	public ReadHBaseIllegalRouteTable(String masterIP, String masterPort,
			String zkIp, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("illegalroute_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject illegalRouteLogHbase(String date1, String date2,
			String nodeIP, String cpVal, List<String> devNameList,
			int iDisplayStart, int iDisplayLength) throws IOException {
		// JSONObject jsonObject=new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);

		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();

		for (String devName : devNameList) {
			Map<Integer, String> resultMap = new HashMap<Integer, String>();
			System.out.println("....." + devName);
			JSONArray propertyArray = new JSONArray();
			String rowkey = date1 + "_" + devName + "_" + nodeIP + "_" + cpVal;
			System.out.println("rowkey" + rowkey);

			Get get = new Get(Bytes.toBytes(rowkey));
			get.setMaxVersions();
			get.setTimeRange(0L, Long.MAX_VALUE);
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("NodeInTraffic"));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("NodeOutTraffic"));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("CP_Length"));
			get.setFilter(filter);

			if (lastRow != null) {
				byte[] StartRow = Bytes.add(lastRow, POSTFIX);
				System.out.println("start row:" + Bytes.toInt(StartRow));
				// scan.setStartRow(StartRow);
			}
			// ResultScanner scanner = table.getScanner(scan);
			int localRows = 1;

			Result result = table.get(get);

			// while ((result = scanner.next()) != null) {
			if (result.list() != null) {
				int index = 1;
				for (KeyValue kv : result.list()) {
					System.out.println(localRows++ + ":" + kv);
					if (localRows == 6) {
						totalRows++;
					}
					// localRows++;
					// double
					// up=Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("cf"),
					// Bytes.toBytes("AppTraffic_UP"))));
					// double
					// dw=Double.parseDouble(Bytes.toString(result.getValue(Bytes.toBytes("cf"),
					// Bytes.toBytes("AppTraffic_DN"))));
					String[] arr = Bytes.toString(kv.getRow()).split("_");
					if (index <= 5) {
						
						resultMap.put(1, devName);

						if (Bytes.toString(kv.getQualifier()).equals(
								"R_StartTime")) {
							resultMap.put(2, Bytes.toString(kv.getValue()));
						}
						if (Bytes.toString(kv.getQualifier()).equals(
								"R_EndTime")) {
							resultMap.put(3, Bytes.toString(kv.getValue()));
						}
						resultMap.put(4, arr[2]);
						if (Bytes.toString(kv.getQualifier()).equals(
								"NodeInTraffic")) {
							resultMap.put(5, Bytes.toString(kv.getValue()));

						}
						if (Bytes.toString(kv.getQualifier()).equals(
								"NodeOutTraffic")) {
							resultMap.put(6, Bytes.toString(kv.getValue()));
						}
						if (Bytes.toString(kv.getQualifier()).equals(
								"CP_Length")) {
							resultMap.put(7, Bytes.toString(kv.getValue()));
						}
						resultMap.put(8, arr[3]);
					}
					// lastRow=result.getRow();
					if (index == 5) {
						total++;
						for (Map.Entry<Integer, String> hMap : resultMap.entrySet()) {
							System.out.print("^^^^ " + hMap.getValue());
							propertyArray.add(hMap.getValue());
						}
						outArray.add(propertyArray);
						index = 0;
						resultMap.clear();
						propertyArray.clear();
					}
					index++;
				}

				
			}
		}
		//

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", total);
		allObject.put("iTotalRecords", total);
		// }

		System.out.println("&&&&:" + allObject);
		System.out.println("total row:" + totalRows);

		return allObject;
	}

	public JSONArray illRouteTrdHbase(String date, String nodeIP, String cpVal,
			String traffType, List<String> devid) throws IOException {
		JSONArray jsonArray = new JSONArray();
		Map<Integer, Double> traffMap = new HashMap<Integer, Double>();
		for (int i = 1; i < 289; i++) {
			traffMap.put(i, 0.0);
		}
		JSONArray sm_jsonArray = new JSONArray();

		for (String dev : devid) {
			String rowkey = date + "_" + dev + "_" + nodeIP + "_" + cpVal;
			System.out.println("------rowkey :" + rowkey);
			Get get = new Get(Bytes.toBytes(rowkey));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(traffType));
			Result rs = table.get(get);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			for (KeyValue kv : rs.raw()) {
				long time = kv.getTimestamp();
				String date2 = sdf.format(new Date(time));
				String arr[] = date2.substring(11, 19).split(":");
				int hour = Integer.parseInt(arr[0]);
				int min = Integer.parseInt(arr[1]);
				int sec = Integer.parseInt(arr[2]);
				int timeStampNum = (int) Math
						.ceil((hour * 3600 + min * 60 + sec) / (60 * 5));
				double tmpSum = 0.0;
				if (traffMap.get(timeStampNum) != null) {
					tmpSum = traffMap.get(timeStampNum);
				}
				traffMap.put(
						timeStampNum,
						tmpSum
								+ Double.parseDouble((Bytes.toString(kv
										.getValue()))));
				System.out.println("***this is the test output***");
				System.out.print(Bytes.toString(kv.getKey()) + "  ");
				System.out.print(timeStampNum + "  ");
				System.out.println(Bytes.toString(kv.getValue()) + "  ");
				System.out.println("***this is the test output***");
			}
		}
		JSONArray jsonarr = new JSONArray();
		if (traffType.equals("NodeInTraffic")) {
			jsonarr.add("接入流量");
		} else if (traffType.equals("NodeOutTraffic")) {
			jsonarr.add("接出流量");
		}

		for (Map.Entry<Integer, Double> entry : traffMap.entrySet()) {
			// System.out.print(entry.getValue() + ",");
			sm_jsonArray.add(entry.getValue());
		}
		jsonarr.add(sm_jsonArray);
		jsonArray.add(jsonarr);
		return jsonArray;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseIllegalRouteTable rirt = new ReadHBaseIllegalRouteTable(
				"10.10.2.41", "60000", "10.10.2.41", "2181");

		List<String> devList = new ArrayList<String>();
		devList.add("ttt");
		devList.add("aaa");
		JSONObject jsonObject = new JSONObject();
		JSONArray jsonArray = new JSONArray();

		try {
			jsonObject = rirt.illegalRouteLogHbase("2014/11/19", "2014/11/19", "10.10.0.117", "APEC", devList, 1,
					2);
			jsonArray = rirt.illRouteTrdHbase("2014/11/19", "10.10.0.117","APEC", "NodeInTraffic", devList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("$$$$******this is the jsonobject*******");
		System.out.println(jsonObject.toString());
		System.out.println("$$$$******this is the jsonarray*******");
		System.out.println(jsonArray.toString());
	}

}
