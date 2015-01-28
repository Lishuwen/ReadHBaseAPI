package com.lsw.day0916;

import java.io.IOException;
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
import org.apache.hadoop.hbase.util.Bytes;

public class ReadHBaseVisitAppTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table;
	private HBaseAdmin admin;

	public ReadHBaseVisitAppTable(String masterIP, String masterPort,
			String zkIp, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("nameduser_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public JSONArray VisitAppUserTrdHbase(String date,
			Map<String, List<String>> appNames, List<String> devid)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result rst = null;

		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, List<String>> appMap : appNames.entrySet()) {

			for (String value : appMap.getValue()) {
				Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
				for (int i = 1; i < 289; i++) {
					versionMap.put(i, 0.0);
				}
				String sm_appName = null;
				for (String dev : devid) {
					// concat the rowkey with the input field
					String rowkey = date + "_" + dev + "_" + appMap.getKey()
							+ "_" + value;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("UserNum"));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);

					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					if (rst != null) {
						for (KeyValue kv : rst.raw()) {
							System.out.println("rowkey is :"
									+ Bytes.toString(kv.getRow()));
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"_");
							sm_appName = arr2[3];
							long time = kv.getTimestamp();
							String date2 = sdf.format(new Date(time));
							String arr[] = date2.substring(11, 19).split(":");
							int hour = Integer.parseInt(arr[0]);
							int min = Integer.parseInt(arr[1]);
							int sec = Integer.parseInt(arr[2]);
							int timeStampNum = (int) Math.ceil((hour * 3600
									+ min * 60 + sec)
									/ (60 * 5));
							double tmpSum = versionMap.get(timeStampNum);
							versionMap.put(
									timeStampNum,
									tmpSum
											+ Double.parseDouble((Bytes
													.toString(kv.getValue()))));
							System.out.println("***this is the test output***");
							System.out
									.print(Bytes.toString(kv.getKey()) + "  ");
							System.out.print(timeStampNum + "  ");
							System.out.println(Bytes.toString(kv.getValue())
									+ "  ");
							System.out.println("***this is the test output***");
						}
					}
				}
				JSONArray jsonarr = new JSONArray();
				// jsonarr.add(value);
				if (sm_appName != null) {
					jsonarr.add(sm_appName);
					// jsonObject.put("appName", value);

					for (int i = 1; i <= versionMap.size(); i++) {
						// System.out.print(entry.getValue() + ",");
						sm_jsonArray.add(versionMap.get(i));
					}
					jsonarr.add(sm_jsonArray);
					sm_jsonArray.clear();
					jsonArray.add(jsonarr);
				}

			}
		}

		return jsonArray;
	}

	// fetch the each piece of data from HBase table
	public JSONArray visitAppUserTrd_SumHbase(String date,
			Map<String, List<String>> appNamesMap, List<String> devNameList)
			throws IOException {

		Map<String, Map<Integer, Double>> aptimeMap = new HashMap<String, Map<Integer, Double>>();
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = null;
		// JSONObject jsonObject = new JSONObject();
		Result rst = null;
		JSONArray jsonArraysm = new JSONArray();
		for (Map.Entry<String, List<String>> appEntry : appNamesMap.entrySet()) {

			sm_jsonArray = new JSONArray();
			Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
			for (int i = 1; i < 289; i++) {
				versionMap.put(i, 0.0);
			}
			String appType = null;
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					// concat the rowkey with the input field
					String rowkey = date + "_" + devName + "_"
							+ appEntry.getKey() + "_" + appName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("UserNum"));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					for (KeyValue kv : rst.raw()) {
						String arr2[] = Bytes.toString(kv.getRow()).split("_");
						appType = arr2[2];
						long time = kv.getTimestamp();
						String date2 = sdf.format(new Date(time));
						String arr[] = date2.substring(11, 18).split(":");
						int hour = Integer.parseInt(arr[0]);
						int min = Integer.parseInt(arr[1]);
						int sec = Integer.parseInt(arr[2]);
						int timeStampNum = (int) Math.ceil((hour * 3600 + min
								* 60 + sec)
								/ (60 * 5));
						versionMap.put(
								timeStampNum,
								versionMap.get(timeStampNum)
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));

						System.out.println("###this is the test output###");
						System.out.print(Bytes.toString(kv.getKey()) + "  ");
						System.out.print(timeStampNum + "  ");
						System.out
								.println(Bytes.toString(kv.getValue()) + "  ");
						System.out
								.println("######this is the test output########");
					}
				}
			}
			if (appType != null) {
				jsonArray.add(appType);
				for (int i = 1; i < versionMap.size(); i++) {

					sm_jsonArray.add(versionMap.get(i));
				}

				jsonArray.add(sm_jsonArray);
				jsonArraysm.add(jsonArray);
				sm_jsonArray.clear();
			}
		}

		// jsonObject.put("appName", appType);
		return jsonArraysm;
	}

	public JSONArray visitAppUserPieHbase(String date,
			Map<String, List<String>> appNames, List<String> devid)
			throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		// System.out.println();
		for (Map.Entry<String, List<String>> appEntry : appNames.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray sm_jsonArrayPie = new JSONArray();
				System.out.println(appName);
				double sum = 0.0;
				for (String devName : devid) {
					String rowkey = date + "_" + devName + "_"
							+ appEntry.getKey() + "_" + appName;
					System.out.println("rowkey" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("UserNum"));
					get.setTimeRange(0L, Long.MAX_VALUE);
					Result rs = null;
					rs = table.get(get);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (KeyValue kv : rs.raw()) {
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					} else {
						sum = 0.0;
					}

					System.out.print(appName + ":" + sum + "    ");

				}
				if (sum != 0.0) {
					sm_jsonArrayPie.add(appName);
					sm_jsonArrayPie.add(sum);
					jsonArrayPie.add(sm_jsonArrayPie);
				}

			}

		}
		return jsonArrayPie;
	}

	public JSONArray visitAppUserPie_SUMHbase(String date,
			Map<String, List<String>> appTypeMap, List<String> devNameList)
			throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					String rowkey = date + "_" + devName + "_"
							+ appEntry.getKey() + "_" + appName;
					System.out.println("rowkey:" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					// get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("UserNum"));
					// get.setTimeRange(0L, Long.MAX_VALUE);
					Result rs = null;
					rs = table.get(get);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (KeyValue kv : rs.raw()) {
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					} else {
						sum = 0.0;
					}

					System.out.print(appName + ":" + sum + "    ");
				}

			}
			if (sum != 0.0) {
				sm_jsonArrayPie.add(appEntry.getKey());
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);
			}

		}
		return jsonArrayPie;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseVisitAppTable rvat = new ReadHBaseVisitAppTable("10.10.2.41",
				"60000", "10.10.2.41", "2181");
		List<String> appList = new ArrayList<String>();
		List<String> devList = new ArrayList<String>();
		devList.add("22");
		devList.add("10");
		Map<String, List<String>> appMap = new HashMap<String, List<String>>();
		appList.add("POCO");
		appList.add("风行HTTP流");
		appMap.put("Web视频类", appList);
		JSONArray jsonArraynew = new JSONArray();
		JSONArray jsonArraynew2 = new JSONArray();
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray1 = new JSONArray();
		try {
			jsonArraynew = rvat.VisitAppUserTrdHbase("2014/11/19", appMap,
					devList);
			jsonArraynew2 = rvat.visitAppUserTrd_SumHbase("2014/11/19", appMap,
					devList);
			jsonArray = rvat
					.visitAppUserPieHbase("2014/11/19", appMap, devList);
			jsonArray1 = rvat.visitAppUserPie_SUMHbase("2014/11/19", appMap,
					devList);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("$$$$******this is the jsonarraynew*******");
		System.out.println(jsonArraynew.toString());
		System.out.println("$$$$******this is the jsonarraynew2*******");
		System.out.println(jsonArraynew2.toString());
		System.out.println("$$$$******this is the jsonarray*******");
		System.out.println(jsonArray.toString());
		System.out.println("$$$$******this is the jsonarray1*******");
		System.out.println(jsonArray1.toString());

	}

}
