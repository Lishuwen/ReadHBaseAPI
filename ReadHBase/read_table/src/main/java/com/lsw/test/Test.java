package com.lsw.test;

import java.io.IOException;
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
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class Test {
	public Configuration config;
	public HTable table;
	public HTable table2;
	public HBaseAdmin admin;

	public Test() {
		config = HBaseConfiguration.create();
		config.set("hbase.master", "10.10.2.41:6000");
		config.set("hbase.zookeeper.property.clientPort", "2181");
		config.set("hbase.zookeeper.quorum", "10.10.2.41");
		try {
			table = new HTable(config, Bytes.toBytes("traffictest"));
			table2 = new HTable(config, Bytes.toBytes("traffictest2"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public void putTable(List recordList) throws IOException {
		for (int i = 0; i < recordList.size() - 1; i++) {
			String[] arr = ((String) recordList.get(i)).split("\t");
			String rowkey = arr[0] + "_" + arr[1] + "_" + arr[2] + "_" + arr[1];
			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppID"),
					Bytes.toBytes(arr[2]));
			String rowkey2 = arr[0];
			Put put2 = new Put(Bytes.toBytes(rowkey2));
			put2.add(Bytes.toBytes("cf"), Bytes.toBytes("AppID"),
					Bytes.toBytes(arr[1]));
			put2.add(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic"),
					Bytes.toBytes(arr[2]));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"),
			// Bytes.toBytes("1970/01/17 10:09:20"));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"),
			// Bytes.toBytes("1970/01/17 10:09:29"));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppUserNum"),
			// Bytes.toBytes(arr[17]));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"),
			// Bytes.toBytes(arr[18]));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_DN"),
			// Bytes.toBytes(arr[19]));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppPacketNum"),
			// Bytes.toBytes(arr[20]));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppSessionsNum"),
			// Bytes.toBytes(arr[21]));
			// put.add(Bytes.toBytes("cf"), Bytes.toBytes("AppNewSessionNum"),
			// Bytes.toBytes(arr[22]));

			table.put(put);
			// table2.put(put2);
		}
	}

	public JSONArray scan() throws IOException {
		// long st=System.currentTimeMillis();
		Scan scanner = new Scan();
		scanner.setStartRow(Bytes.toBytes("app_1"));
		scanner.setStopRow(Bytes.toBytes("app_9999"));
		scanner.setTimeRange(0L, Long.MAX_VALUE);
		scanner.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic"));
		scanner.setBatch(0);
		scanner.setCaching(100000);
		scanner.setMaxVersions();
		ResultScanner reScanner = table.getScanner(scanner);
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i <= 999; i++) {
			versionMap.put(i, 0.0);
		}
		double tmpSum = 0.0;
		int index = 0;
		for (Result rs : reScanner) {
			for (KeyValue kv : rs.raw()) {
				String arr[] = Bytes.toString(kv.getKey()).split("_");

				versionMap.put(
						Integer.parseInt(arr[1]),
						tmpSum
								+ Double.parseDouble((Bytes.toString(kv
										.getValue()))));

				System.out.println("###this is the test output###");
				System.out.print(Bytes.toString(kv.getKey()) + "  ");

				System.out.println(Bytes.toString(kv.getValue()) + "  ");
				System.out.println("######this is the test output########");
				index++;
			}
		}
		JSONArray jsonArray = new JSONArray();
		for (int i = 1; i <= versionMap.size(); i++) {
			jsonArray.add(versionMap.get(i));
		}
		reScanner.close();
		return jsonArray;
		
		// long en=System.currentTimeMillis();
		// System.out.println(en-st);
	}

	// public static void main(String []args){
	// try {
	// Process p=Runtime.getRuntime().exec("/home/cyt/aaaa/k.sh");
	// } catch (IOException e) {
	// // TODO Auto-generated catch block
	// e.printStackTrace();
	// }
	public JSONArray GetData() throws IOException {
		Get get = new Get(Bytes.toBytes("app"));
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppID"));
		get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic"));
		Result rs = table2.get(get);
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i <= 999; i++) {
			versionMap.put(i, 0.0);
		}
		double tmpSum = 0.0;
		int index = 0;
		for (KeyValue kv : rs.raw()) {
			if (index <= 576) {
				int day = 0;
				if (Bytes.toString(kv.getQualifier()).equals("AppID")) {
					day = Integer.parseInt(Bytes.toString(kv.getValue()));
				}
				versionMap.put(
						day,
						tmpSum
								+ Double.parseDouble((Bytes.toString(kv
										.getValue()))));

			}
			if (index == 576) {
				index = 0;
			}
			System.out.println("###this is the test output###");
			System.out.print(Bytes.toString(kv.getKey()) + "  ");

			System.out.println(Bytes.toString(kv.getValue()) + "  ");
			System.out.println("######this is the test output########");
			index++;
		}
		JSONArray jsonArray = new JSONArray();
		for (int i = 1; i <= versionMap.size(); i++) {
			jsonArray.add(versionMap.get(i));
		}

		return jsonArray;
	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		Test h = new Test();
		// hs.UidFilter();
		// BufferedReader br = null;
		// String str = null;
		// List<String> strList = new ArrayList<String>();
		// br = new BufferedReader(new InputStreamReader(new FileInputStream(
		// args[0])));
		// while ((str = br.readLine()) != null) {
		// // String []arr=str.split("\001");
		// strList.add(str);
		// }
		// h.putTable(strList);
		JSONArray jsonArray1 = new JSONArray();
		long startTime = System.currentTimeMillis();
		jsonArray1 = h.scan();
		long endTime = System.currentTimeMillis();
		System.out.println(jsonArray1);
		System.out.println("extTime----->" + (endTime - startTime));
		
//		JSONArray jsonArray = new JSONArray();
//		long startTime = System.currentTimeMillis();
//		jsonArray = h.GetData();
//		long endTime = System.currentTimeMillis();
//		System.out.println(jsonArray);
//		System.out.println("extTime----->" + (endTime - startTime));
	}
}
