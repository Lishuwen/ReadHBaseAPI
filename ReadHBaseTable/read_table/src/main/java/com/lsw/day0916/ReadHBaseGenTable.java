package com.lsw.day0916;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PageFilter;
import org.apache.hadoop.hbase.util.Bytes;

public class ReadHBaseGenTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table;
	private HBaseAdmin admin;

	public ReadHBaseGenTable(String masterIP, String masterPort, String zkIP,
			String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIP);

		try {
			table = new HTable(config, Bytes.toBytes("genetraffic_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public JSONArray genFlowTotalInfoHbase() throws IOException {
		JSONArray jsonArray = new JSONArray();
		JSONArray injsonArray = new JSONArray();
		Scan scan = new Scan();
		scan.setMaxVersions();
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"));
		scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_DN"));
		scan.setTimeRange(0L, Long.MAX_VALUE);
		ResultScanner reScan = table.getScanner(scan);
		double upSum = 0.0, downSum = 0.0, upSumTB = 0.0, downSumTB = 0.0;
		for (Result rs : reScan) {
			upSum += Double.parseDouble(Bytes.toString(rs.getValue(
					Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_UP"))));
			downSum += Double.parseDouble(Bytes.toString(rs.getValue(
					Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_DN"))));

		}
		// upSumTB = Double.parseDouble(String.format("%.4f", upSum
		// / (1024 * 1024)));
		// downSumTB = Double.parseDouble(String.format("%.4f", upSum
		// / (1024 * 1024)));
		upSumTB = Double.parseDouble(String.format("%.4f", upSum));
		downSumTB = Double.parseDouble(String.format("%.4f", downSum));
		injsonArray.add(upSumTB);
		injsonArray.add(downSumTB);
		jsonArray.add(injsonArray);
		return jsonArray;

	}

	public JSONArray genFlowL2Hbase(String date, String userGroupId,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {

		Map<String, Map<Integer, Double>> aptimeMap = new HashMap<String, Map<Integer, Double>>();
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		// JSONObject jsonObject = new JSONObject();
		Result rst = null;

		for (Map.Entry<String, List<String>> typeEntry : appTypeMap.entrySet()) {
			JSONArray sm_jsonArray2 = new JSONArray();
			for (String appName : typeEntry.getValue()) {
				Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
				for (int i = 1; i < 289; i++) {
					versionMap.put(i, 0.0);
				}
				double sum = 0.0;
				String sm_appName = null;
				for (String devName : devNameList) {
					// concat the rowkey with the input field
					String rowkey = userGroupId + "_" + date + "_"
							+ typeEntry.getKey() + "_" + appName + "_"
							+ devName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					for (KeyValue kv : rst.raw()) {
						String[] arr = Bytes.toString(kv.getRow()).split("_");
						sm_appName = arr[3];
						long time = kv.getTimestamp();
						String date2 = sdf.format(new Date(time));
						String arr2[] = date2.substring(11, 18).split(":");
						int hour = Integer.parseInt(arr2[0]);
						int min = Integer.parseInt(arr2[1]);
						int sec = Integer.parseInt(arr2[2]);
						int timeStampNum = (int) Math.ceil((hour * 3600 + min
								* 60 + sec)
								/ (60 * 5));
						sum += Double
								.parseDouble((Bytes.toString(kv.getValue())));
						double tmpSum = versionMap.get(timeStampNum);
						versionMap.put(
								timeStampNum,
								tmpSum
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));
						System.out
								.println("###this is the optional test output###");
						System.out.print(Bytes.toString(kv.getKey()) + "  ");
						System.out.print(timeStampNum + "  ");
						System.out
								.println(Bytes.toString(kv.getValue()) + "  ");
						System.out
								.println("######this is the optional test output########");
					}
				}

				// for (Map.Entry<Integer, Double> entry :
				// versionMap.entrySet()) {
				// sm_jsonArray.clear();
				// sm_jsonArray.add(entry.getValue());
				// }
				// jsonObject.put(appType, sm_jsonArray);

			}
			for (Map.Entry<String, Map<Integer, Double>> tmpapTime : aptimeMap
					.entrySet()) {
				sm_jsonArray2.add(tmpapTime.getKey());
				for (int i = 1; i <= tmpapTime.getValue().size(); i++) {
					// sm_jsonArray.clear();
					sm_jsonArray.add(tmpapTime.getValue().get(i));
				}
				sm_jsonArray2.add(sm_jsonArray);
				jsonArray.add(sm_jsonArray2);
				sm_jsonArray.clear();
			}

		}
		// jsonObject.put("appName", appType);

		return jsonArray;

	}

	public JSONArray genFlowL2_SumHbase(String date, String userGroup,
			Map<String, List<String>> appNamesMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
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
			String sm_appType = null;
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					// concat the rowkey with the input field
					String rowkey = userGroup + "_" + date + "_"
							+ appEntry.getKey() + "_" + appName + "_" + devName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					for (KeyValue kv : rst.raw()) {
						String arr2[] = Bytes.toString(kv.getRow()).split("_");
						sm_appType = arr2[2];
						long time = kv.getTimestamp();
						String date2 = sdf.format(new Date(time));
						String arr[] = date2.substring(11, 18).split(":");
						int hour = Integer.parseInt(arr[0]);
						int min = Integer.parseInt(arr[1]);
						int sec = Integer.parseInt(arr[2]);
						int timeStampNum = (int) Math.ceil((hour * 3600 + min
								* 60 + sec)
								/ (60 * 5));
						double tmpSum = versionMap.get(timeStampNum);
						versionMap.put(
								timeStampNum,
								tmpSum
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
			if (sm_appType != null) {
				jsonArray.add(sm_appType);
				for (int i = 1; i <= versionMap.size(); i++) {
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

	public JSONArray genFlowPieHbase(String date, String userGroupId,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		// public JSONArray genFlowPieHbase() throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		// System.out.println();
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray sm_jsonArrayPie = new JSONArray();
				System.out.println(appName);
				double sum = 0.0;
				String sm_appName = null;
				for (String devName : devNameList) {
					String rowkey = userGroupId + "_" + date + "_"
							+ appEntry.getKey() + "_" + appName + "_" + devName;
					System.out.println("rowkey" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					get.setTimeRange(0L, Long.MAX_VALUE);
					Result rs = null;
					rs = table.get(get);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (KeyValue kv : rs.raw()) {
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"_");
							sm_appName = arr2[3];
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					}

					System.out.print(appName + ":" + sum + "    ");

				}
				if (sm_appName != null) {
					sm_jsonArrayPie.add(sm_appName);
					sm_jsonArrayPie.add(sum);
					jsonArrayPie.add(sm_jsonArrayPie);
				}

			}

		}
		return jsonArrayPie;
	}

	public JSONArray genFlowPie_SumHbase(String date, String userGroupId,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			String appType = null;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					String rowkey = userGroupId + "_" + date + "_"
							+ appEntry.getKey() + "_" + appName + "_" + devName;
					System.out.println("rowkey:" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					// get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					// get.setTimeRange(0L, Long.MAX_VALUE);
					Result rs = null;
					rs = table.get(get);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (KeyValue kv : rs.raw()) {
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"_");
							appType = arr2[2];
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					} 

					System.out.print(appName + ":" + sum + "    ");
				}

			}
			if (appType!=null) {
				sm_jsonArrayPie.add(appType);
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);
				
			}

		}
		return jsonArrayPie;
	}

	public JSONArray genFlowTop10Hbase(String date, String userGroupId,
			Map<String, List<String>> appMap, String appTraffic_Field,
			List<String> devNameList) throws IOException {
		Map<Double, ArrayList<JSONArray>> appName_numMap = new HashMap<Double, ArrayList<JSONArray>>();
		// ArrayList<JSONArray> appList = new ArrayList<JSONArray>();
		JSONArray top_jsonArray = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appMap.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray one_jsonArray = new JSONArray();
				System.out.println(appName);
				System.out.println("&&&" + one_jsonArray);
				Map<JSONArray, Double> tmpMap = new HashMap<JSONArray, Double>();
				for (String devName : devNameList) {
					double sum = 0.0;
					// one_jsonArray = new JSONArray();
					String rowkey = userGroupId + "_" + date + "_"
							+ appEntry.getKey() + "_" + appName + "_" + devName;
					System.out.println("rowkey:" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					// get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					// get.setTimeRange(0L, Long.MAX_VALUE);
					Result rst = table.get(get);
					if (rst != null) {
						if (!tmpMap.containsKey(one_jsonArray)) {
							one_jsonArray.add(appEntry.getKey());
							one_jsonArray.add(appName);
							for (KeyValue kv : rst.raw()) {
								tmpMap.put(one_jsonArray, Double
										.parseDouble(Bytes.toString(kv
												.getValue())));
							}
						} else {
							for (KeyValue kv : rst.raw()) {
								tmpMap.put(
										one_jsonArray,
										Double.parseDouble(Bytes.toString(kv
												.getValue()))
												+ tmpMap.get(one_jsonArray));
							}
						}
					}
					// for (KeyValue kv : rst.raw()) {
					// String arr[]=kv.getKeyString().split("_");
					// one_jsonArray.add(arr[2]);
					// one_jsonArray.add(arr[3]);
					// tmpMap.put(one_jsonArray, Double
					// .parseDouble(Bytes.toString(kv
					// .getValue())));
					// sum+=Double
					// .parseDouble(Bytes.toString(kv
					// .getValue()));
					// }
					// if(appName_numMap.get(sum)!=null){
					//
					// }else{
					//
					// }
					System.out.print(appName + ":" + sum + "    ");
					// appList.add(one_jsonArray);
				}
				for (Map.Entry<JSONArray, Double> e : tmpMap.entrySet()) {
					ArrayList<JSONArray> tmpList = new ArrayList<JSONArray>();
					if (appName_numMap.get(e.getValue()) != null) {
						ArrayList<JSONArray> tmpList2 = appName_numMap.get(e
								.getValue());
						tmpList2.add(e.getKey());
						appName_numMap.put(e.getValue(), tmpList2);
					} else {
						tmpList.add(e.getKey());
						appName_numMap.put(e.getValue(), tmpList);
					}
				}
			}
		}
		// sort the sum of appName ,and get the top 10
		Double[] sm_value = appName_numMap.keySet().toArray(
				new Double[appName_numMap.size()]);
		Arrays.sort(sm_value);
		JSONArray tmpjArray = new JSONArray();
		System.out.println(",,,,," + sm_value.length);
		// System.out.println(",,,,," + appName_numMap.get(0.0).get(0));

		int count = 1;
		for (int i = sm_value.length - 1; i >= 0; i--) {
			if (count <= 10) {
				System.out.println("the key of the map is: " + i);
				for (JSONArray oneAppName : appName_numMap.get(sm_value[i])) {
					oneAppName.add(sm_value[i]);
					top_jsonArray.add(oneAppName);
				}
			}
			count++;
		}

		return top_jsonArray;
	}

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject genFlowDetailsHbase(String date,
			Map<String, String> userGroupId,
			Map<String, List<String>> appTypeMap, List<String> devNameList,
			int iDisplayStart, int iDisplayLength) throws IOException {
		// JSONObject jsonObject=new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();

		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			// resultMap.put(5, appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				// resultMap.put(6, appName);
				for (Map.Entry<String, String> userEntry : userGroupId
						.entrySet()) {
					// resultMap.put(4, userEntry.getKey());
					for (String devName : devNameList) {
						Map<Integer, String> resultMap = new HashMap<Integer, String>();
						System.out.println("....." + devName);
						JSONArray propertyArray = new JSONArray();
						String rowkey = userEntry.getKey() + "_" + date + "_"
								+ appEntry.getKey() + "_" + appName + "_"
								+ devName;
						System.out.println("rowkey" + rowkey);

						Get get = new Get(Bytes.toBytes(rowkey));
						get.setMaxVersions();
						get.setTimeRange(0L, Long.MAX_VALUE);
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("R_StartTime"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("R_EndTime"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("AppUserNum"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("AppTraffic_UP"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("AppTraffic_DN"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("AppPacketsNum"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("AppSessionNum"));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("AppNewSessionNum"));
						get.setFilter(filter);
						// while (true) {
						// Scan scan = new Scan(get);
						// scan.getMaxVersions();
						// scan.setTimeRange(0L, Long.MAX_VALUE);
						// scan.setFilter(filter);

						if (lastRow != null) {
							byte[] StartRow = Bytes.add(lastRow, POSTFIX);
							System.out.println("start row:"
									+ Bytes.toInt(StartRow));
							// scan.setStartRow(StartRow);
						}
						// ResultScanner scanner = table.getScanner(scan);
						int localRows = 1;
						Result result = table.get(get);
						System.out.println("Log Detail");
						System.out.println(result.toString());
						if (result.list() != null) {
							int index = 1;
							for (KeyValue kv : result.list()) {

								System.out.print(index + "_");
								System.out.println(localRows++ + ":" + kv);
								if (localRows == 8) {
									totalRows++;
								}
								String[] arr = Bytes.toString(kv.getRow())
										.split("_");
								if (index <= 8) {
									resultMap.put(4, userGroupId.get(arr[0]));
									resultMap.put(5, arr[2]);
									resultMap.put(6, arr[3]);
									System.out.println("this is tmp output");
									System.out
											.println("R_StartTime:"
													+ Bytes.toString(result.getValue(
															Bytes.toBytes("cf"),
															Bytes.toBytes("R_StartTime"))));
									System.out
											.println("AppUserNum:"
													+ Bytes.toString(result.getValue(
															Bytes.toBytes("cf"),
															Bytes.toBytes("AppUserNum"))));

									resultMap.put(1, devName);

									if (Bytes.toString(kv.getQualifier())
											.equals("R_StartTime")) {
										resultMap.put(2,
												Bytes.toString(kv.getValue()));
									}
									if (Bytes.toString(kv.getQualifier())
											.equals("R_EndTime")) {
										resultMap.put(3,
												Bytes.toString(kv.getValue()));
									}
									if (Bytes.toString(kv.getQualifier())
											.equals("AppUserNum")) {
										resultMap.put(7,
												Bytes.toString(kv.getValue()));

									}
									if (Bytes.toString(kv.getQualifier())
											.equals("AppTraffic_UP")) {
										resultMap.put(8,
												Bytes.toString(kv.getValue()));
									}
									if (Bytes.toString(kv.getQualifier())
											.equals("AppTraffic_DN")) {
										resultMap.put(9,
												Bytes.toString(kv.getValue()));
									}
									if (Bytes.toString(kv.getQualifier())
											.equals("AppPacketsNum")) {
										resultMap.put(10,
												Bytes.toString(kv.getValue()));
									}
									if (Bytes.toString(kv.getQualifier())
											.equals("AppSessionNum")) {
										resultMap.put(11,
												Bytes.toString(kv.getValue()));
									}
									if (Bytes.toString(kv.getQualifier())
											.equals("AppNewSessionNum")) {
										resultMap.put(12,
												Bytes.toString(kv.getValue()));
									}
									// lastRow=result.getRow();

								}
								if (index == 8) {
									total++;
									for (int i=1;i<=resultMap.size();i++) {
										System.out.print("^^^^ "
												+ resultMap.get(i));
										propertyArray.add(resultMap.get(i));
									}
									outArray.add(propertyArray);

									index = 0;
									resultMap.clear();
									propertyArray.clear();
								}
								index++;
							}

						}

						// scanner.close();
						totalRows = 0;
						if (localRows == 0) {
							break;
						}
						// }
					}

				}

				System.out.println("&&&&:" + allObject);
				System.out.println("total row:" + totalRows);
			}
		}
		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", total);
		allObject.put("iTotalRecords", total);
		return allObject;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseGenTable rht = new ReadHBaseGenTable("10.10.2.41", "60000",
				"10.10.2.41", "2181");
		List<String> appList = new ArrayList<String>();
		List<String> devList = new ArrayList<String>();
		devList.add("gn-1");
		Map<String, List<String>> appMap = new HashMap<String, List<String>>();
		appList.add("POCO");
		appList.add("lllll");
		appMap.put("P2PDownload", appList);
		Map<String, String> appMap2 = new HashMap<String, String>();
		appMap2.put("0", "海2区");
		JSONArray jsonArraynew = new JSONArray();
		JSONArray jsonArraynew2 = new JSONArray();
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray1 = new JSONArray();
		JSONArray jsonArray2 = new JSONArray();
		JSONArray jsonArray3 = new JSONArray();
		JSONObject jsonObject = new JSONObject();
		try {
			jsonArraynew = rht.genFlowL2Hbase("1970/01/17", "0", appMap,
					"AppTraffic_UP", devList);
			jsonArraynew2 = rht.genFlowL2_SumHbase("1970/01/17", "0", appMap,
					"AppTraffic_UP", devList);
			jsonArray = rht.genFlowPieHbase("1970/01/17", "0", appMap,
					"AppTraffic_UP", devList);
			jsonArray1 = rht.genFlowPie_SumHbase("1970/01/17", "0", appMap,
					"AppTraffic_UP", devList);
			jsonArray2 = rht.genFlowTop10Hbase("1970/01/17", "0", appMap,
					"AppTraffic_UP", devList);
			jsonArray3 = rht.genFlowTotalInfoHbase();
			jsonObject = rht.genFlowDetailsHbase("1970/01/17", appMap2, appMap,
					devList, 1, 2);
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
		System.out.println("$$$******this is the jsonarray2*******");
		System.out.println(jsonArray2.toString());
		System.out.println("$$$$******this is the jsonarray3*******");
		System.out.println(jsonArray3.toString());
		System.out.println("$$$$******this is the jsonobject*******");
		System.out.println(jsonObject.toString());
		// System.out.println("#########this is the Sum_jsonarray##########");
		// System.out.println(jaSum.toString());
	}

}
