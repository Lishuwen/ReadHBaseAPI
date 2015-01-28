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
import com.lsw.day0916.*;
public class ReadHBaseAttackTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table;
	private HBaseAdmin admin;

	public ReadHBaseAttackTable(String masterIP, String masterPort,
			String zkIP, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIP);

		try {
			table = new HTable(config, Bytes.toBytes("ddosattack_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public JSONArray attackChart5MHbase(String userGroupId, String attackFiled,
			String attackType, String date, ArrayList<String> devNameList)
			throws IOException {

		// for (Map.Entry<Integer, Double> entry : versionMap.entrySet()) {
		// versionMap.put(entry.getKey(), 0.0);
		// }

		JSONArray jsonArray = new JSONArray();
		Result rst = null;

		JSONArray sm_jsonArray = new JSONArray();
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i < 289; i++) {
			versionMap.put(i, 0.0);
		}
		for (String devname : devNameList) {
			// concat the rowkey with the input field
			String rowkey = devname + "_" + userGroupId + "_" + attackType
					+ "_" + date;
			Get get = new Get(Bytes.toBytes(rowkey));
			get.getMaxVersions();
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(attackFiled));
			get.setTimeRange(0L, Long.MAX_VALUE);
			rst = table.get(get);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			for (KeyValue kv : rst.raw()) {
				long time = kv.getTimestamp();
				String date2 = sdf.format(new Date(time));
				String arr[] = date2.substring(11, 19).split(":");
				int hour = Integer.parseInt(arr[0]);
				int min = Integer.parseInt(arr[1]);
				int sec = Integer.parseInt(arr[2]);
				int timeStampNum = (int) Math
						.ceil((hour * 3600 + min * 60 + sec) / (60 * 5));
				versionMap.put(timeStampNum,
						Double.parseDouble((Bytes.toString(kv.getValue()))));
				System.out.println("date2:" + date2);
				System.out.println("***this is the test output***");
				System.out.print(Bytes.toString(kv.getKey()) + "  ");
				System.out.print(timeStampNum + "  ");
				System.out.println(Bytes.toString(kv.getValue()) + "  ");
				System.out.println("***this is the test output***");

			}
			JSONArray jsonarr = new JSONArray();
			// jsonObject.put("appName", value);
			// for (Map.Entry<Integer, Double> entry : versionMap.entrySet()) {
			// // System.out.print(entry.getValue() + ",");
			// sm_jsonArray.add(entry.getValue());
			// }
			for (int i = 1; i < versionMap.size(); i++) {
				sm_jsonArray.add(versionMap.get(i));
			}
			// jsonarr.add(value);
			// jsonarr.add(userGroupId);
			jsonarr.add(sm_jsonArray);
			jsonArray.add(jsonarr);
		}
		return jsonArray;
	}

	public JSONObject attackAreaHbase(String userGroupId, String date1,
			String date2, String attackType, ArrayList<String> devNameList,
			int iDisplayLengthStart, int iDisplayLength) throws IOException {
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayLengthStart);
		System.out.println(lastRow.length);
		Map<String, AttackBean> attackMap = new HashMap<String, AttackBean>();
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		JSONObject jsonObject = new JSONObject();
		Result rst = null;
		Result rst2 = null;
		Result rst3 = null;
		for (String devName : devNameList) {
			// concat the rowkey with the input field
			String rowkey = devName + "_" + userGroupId + "_" + attackType
					+ "_" + date1;
			Get get = new Get(Bytes.toBytes(rowkey));
			get.getMaxVersions();
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackAreaName"));
			get.setTimeRange(0L, Long.MAX_VALUE);
			Get get2 = new Get(Bytes.toBytes(rowkey));
			get2.getMaxVersions();
			get2.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackTraffic"));
			get2.setTimeRange(0L, Long.MAX_VALUE);
			Get get3 = new Get(Bytes.toBytes(rowkey));
			get3.getMaxVersions();
			get3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackNum"));
			get3.setTimeRange(0L, Long.MAX_VALUE);
			get.setFilter(filter);
			get2.setFilter(filter);
			get3.setFilter(filter);
			rst = table.get(get);
			rst2 = table.get(get2);
			rst3 = table.get(get3);
			double num = 0.0;
			System.out.println("###this is the test output###");
			System.out.println("######this is the test output########");
			if (lastRow != null) {
				byte[] StartRow = Bytes.add(lastRow, POSTFIX);
				System.out.println("start row:" + Bytes.toInt(StartRow));
				// scan.setStartRow(StartRow);
			}
			List<KeyValue> list = rst.list();
			List<KeyValue> list2 = rst.list();
			List<KeyValue> list3 = rst.list();
		}
		List<KeyValue> list = rst.list();
		List<KeyValue> list2 = rst2.list();
		List<KeyValue> list3 = rst3.list();
		if (list != null) {
			for (int i = 0; i <= list.size() - 1; i++) {
				String key = Bytes.toString(list.get(i).getValue());
				double NumValue = Double.parseDouble(Bytes.toString(list3
						.get(i).getValue()));
				double TrafficValue = Double.parseDouble(Bytes.toString(list2
						.get(i).getValue()));
				AttackBean ak = new AttackBean();
				if (attackMap.get(key) != null) {
					double tmpNum = attackMap.get(key).getAttackNum();
					ak.setAttackTraffic(NumValue + tmpNum);
					double tmpTraffic = attackMap.get(key).getAttackTraffic();
					ak.setAttackNum((TrafficValue + tmpTraffic));
					attackMap.put(key, ak);
				} else {
					ak.setAttackTraffic(TrafficValue);
					ak.setAttackNum(NumValue);
					attackMap.put(key, ak);
				}
			}

		}
		if (list != null) {
			totalRows += list.size();
		}
		// jsonArray.add(appType);
		for (Map.Entry<String, AttackBean> attEntry : attackMap.entrySet()) {
			sm_jsonArray.add("<a href=\"#\" onclick=\"load_AreaLog('"
					+ attEntry.getKey() + "')\">" + attEntry.getKey() + "</a>");
			sm_jsonArray.add(attEntry.getValue().getAttackNum());
			sm_jsonArray.add(attEntry.getValue().getAttackTraffic());
			jsonArray.add(sm_jsonArray);
			sm_jsonArray.clear();
		}
		jsonObject.put("aaData", jsonArray);
		jsonObject.put("iTotalDisplayRecords", totalRows);
		jsonObject.put("iTotalRecords", totalRows);
		return jsonObject;

	}

	public JSONObject attackAreaLogHbase(Map<String, String> userGroupId,
			String date1, String date2, Map<String, String> attackType,
			ArrayList<String> devNameList, String areaName,
			int iDisplayLengthStart, int iDisplayLength) throws IOException {
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayLengthStart);
		System.out.println(lastRow.length);
		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();
		for (Map.Entry<String, String> attackEntry : attackType.entrySet()) {
			for (Map.Entry<String, String> userEntry : userGroupId.entrySet()) {
				for (String devName : devNameList) {
					JSONArray propertyArray = new JSONArray();
					// concat the rowkey with the input field
					String rowkey = devName + "_" + userEntry.getKey() + "_"
							+ attackEntry.getKey() + "_" + date1;
					System.out.println("&&AreaLog :" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("Attack_StartTime"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("Attack_EndTime"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AttackAreaName"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AttackNum"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AttackTraffic"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AttackAreaNum"));
					get.setTimeRange(0L, Long.MAX_VALUE);
					get.setFilter(filter);
					Result rst = table.get(get);
					System.out.println("######this is the test output########");
					if (lastRow != null) {
						byte[] StartRow = Bytes.add(lastRow, POSTFIX);
						System.out
								.println("start row:" + Bytes.toInt(StartRow));
						// scan.setStartRow(StartRow);
					}
					if (rst.list() != null) {

						int index = 1;
						Map<Integer, String> resultMap = new HashMap<Integer, String>();
						for (KeyValue kv : rst.list()) {

							System.out.print(index + "_");
							String[] arr = Bytes.toString(kv.getRow()).split(
									"_");
							if (index <= 6) {
								System.out.print("rst VALUE : "
										+ rst.toString());
								resultMap.put(4, userGroupId.get(arr[1]));
								resultMap.put(5, attackType.get(arr[2]));
								System.out.println("this is tmp output");
								resultMap.put(1, arr[0]);
								if (Bytes.toString(kv.getQualifier()).equals(
										"Attack_StartTime")) {
									resultMap.put(2,
											Bytes.toString(kv.getValue()));
									System.out.println("......"
											+ resultMap.get(2));
								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"Attack_EndTime")) {
									resultMap.put(3,
											Bytes.toString(kv.getValue()));
									System.out.println("......"
											+ resultMap.get(3));
								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"AttackAreaName")) {

									resultMap.put(6,
											Bytes.toString(kv.getValue()));
									System.out.println("......"
											+ resultMap.get(6));

								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"AttackNum")) {
									resultMap.put(7,
											Bytes.toString(kv.getValue()));
									System.out.println("......"
											+ resultMap.get(7));
								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"AttackTraffic")) {
									resultMap.put(8,
											Bytes.toString(kv.getValue()));
									System.out.println("......"
											+ resultMap.get(8));
								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"AttackAreaNum")) {
									resultMap.put(9,
											Bytes.toString(kv.getValue()));
									System.out.println("......"
											+ resultMap.get(9));
								}

								// lastRow=result.getRow();

							}
							for (int i = 1; i <= 9; i++) {
								System.out.println(i + "......outer : "
										+ resultMap.get(i));
							}
							System.out.println("......outer : "
									+ resultMap.size());

							if (index == 6) {
								
								System.out.println(areaName);
								System.out.println("AreaName ::"
										+ resultMap.get(6));
								if (resultMap.get(6).equals(areaName)) {
									total++;
									for (int i=1;i<resultMap.size();i++) {
										System.out.print("^^^^ "
												+ resultMap.get(i));
										propertyArray.add(resultMap.get(i));
									}
									outArray.add(propertyArray);

									index = 0;
									resultMap.clear();
									propertyArray.clear();
								}

							}
							index++;
						}
					}

				}

				// jsonArray.add(appType);

				allObject.put("aaData", outArray);
				allObject.put("iTotalDisplayRecords", total);
				allObject.put("iTotalRecords", total);
			}
		}
		return allObject;
	}

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject attackLogDetailsHbase(Map<String, String> userGroupId,
			String date1, String date2, Map<String, String> appAttTypeMap,
			List<String> devNameList, int iDisplayStart, int iDisplayLength)
			throws IOException {
		// JSONObject jsonObject=new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);

		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();

		for (Map.Entry<String, String> appAttEntry : appAttTypeMap.entrySet()) {
			// resultMap.put(5, appEntry.getKey());
			// resultMap.put(6, appName);
			for (Map.Entry<String, String> userEntry : userGroupId.entrySet()) {
				// resultMap.put(4, userEntry.getKey());
				for (String devName : devNameList) {
					Map<Integer, String> resultMap = new HashMap<Integer, String>();
					System.out.println("....." + devName);
					JSONArray propertyArray = new JSONArray();
					String rowkey = devName + "_" + userEntry.getKey() + "_"
							+ appAttEntry.getKey() + "_" + date1;
					System.out.println("rowkey" + rowkey);

					Get get = new Get(Bytes.toBytes(rowkey));
					get.setMaxVersions();
					get.setTimeRange(0L, Long.MAX_VALUE);
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AttackAreaNum"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("Attack_StartTime"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("Attack_EndTime"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AppAttackTraffic"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AppAttackRate"));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes("AttackAreaName"));

					get.setFilter(filter);

					if (lastRow != null) {
						byte[] StartRow = Bytes.add(lastRow, POSTFIX);
						System.out
								.println("start row:" + Bytes.toInt(StartRow));
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
							String[] arr = Bytes.toString(kv.getRow()).split(
									"_");
							if (index <= 6) {
								resultMap.put(4, userGroupId.get(arr[1]));
								resultMap.put(5, appAttTypeMap.get(arr[2]));
								resultMap.put(1, devName);

								if (Bytes.toString(kv.getQualifier()).equals(
										"Attack_StartTime")) {
									resultMap.put(2,
											Bytes.toString(kv.getValue()));
								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"Attack_EndTime")) {
									resultMap.put(3,
											Bytes.toString(kv.getValue()));
								}

								if (Bytes.toString(kv.getQualifier()).equals(
										"AppAttackTraffic")) {
									resultMap.put(6,
											Bytes.toString(kv.getValue()));

								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"AppAttackRate")) {
									resultMap.put(7,
											Bytes.toString(kv.getValue()));
								}
								if (Bytes.toString(kv.getQualifier()).equals(
										"AttackAreaNum")) {
									resultMap.put(8,
											Bytes.toString(kv.getValue()));
								}

								// lastRow=result.getRow();
							}

							if (index == 6) {
								total++;
								for (int i=1;i<=resultMap.size();i++) {
									System.out.print("^^^^ " + resultMap.get(i));
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
					//
					// if (totalRows != 0) {
					// for (Map.Entry<Integer, String> hMap : resultMap
					// .entrySet()) {
					// System.out.print("^^^^ " + hMap.getValue());
					// propertyArray.add(hMap.getValue());
					// }
					// outArray.add(propertyArray);
					// allObject.put("aaData", outArray);
					// } else {
					//
					// allObject.put("aaData", outArray);
					// }
					allObject.put("aaData", outArray);
					allObject.put("iTotalDisplayRecords", total);
					allObject.put("iTotalRecords", total);
					// scanner.close();
					totalRows = 0;
					if (localRows == 0) {
						break;
					}
					// }

				}
				System.out.println("&&&&:" + allObject);
				System.out.println("total row:" + totalRows);
			}
		}
		return allObject;
	}

	public JSONArray attackTotalHbase(String userGroup, String date1,
			String date2, String appAttType, ArrayList<String> devName)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		double AttackNum = 0.0, AttackTraffic = 0.0;
		JSONArray sjsonArray = new JSONArray();
		for (String dev : devName) {

			String rowkey = dev + "_" + userGroup + "_" + appAttType + "_"
					+ date1;
			Get get = new Get(Bytes.toBytes(rowkey));
			get.setMaxVersions();
			get.setTimeRange(0L, Long.MAX_VALUE);
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackNum"));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackTraffic"));
			Result rs = null;
			rs = table.get(get);
			for (KeyValue kv : rs.raw()) {
				if (Bytes.toString(kv.getQualifier()).equals("AttackNum")) {
					AttackNum += Double.parseDouble(Bytes.toString(kv
							.getValue()));
				}
				if (Bytes.toString(kv.getQualifier()).equals("AttackTraffic")) {
					AttackTraffic += Double.parseDouble(Bytes.toString(kv
							.getValue()));
				}
			}
		}
		sjsonArray.add(AttackNum);
		sjsonArray.add(1);
		sjsonArray.add(AttackTraffic);
		jsonArray.add(sjsonArray);
		return jsonArray;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseAttackTable rb = new ReadHBaseAttackTable("10.10.2.41",
				"60000", "10.10.2.41", "2181");
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray1 = new JSONArray();
		// JSONArray jsonArray2=new JSONArray();
		JSONObject jsonObject = new JSONObject();
		JSONObject jsonObject1 = new JSONObject();
		JSONObject jsonObject2 = new JSONObject();
		// List<String> appList = new ArrayList<String>();
		ArrayList<String> devList = new ArrayList<String>();
		devList.add("gn-1");
		Map<String, String> appMap = new HashMap<String, String>();
		// appList.add("POCO");
		// / appList.add("lllll");
		appMap.put("1", "applicationType");
		Map<String, String> userMap = new HashMap<String, String>();

		userMap.put("0", "哈哈哈");
		try {
			jsonArray = rb.attackChart5MHbase("0", "AppAttackRate", "1",
					"2014/11/15", devList);
			jsonObject1 = rb.attackAreaHbase("0", "2014/11/15", "2014/11/15",
					"1", devList, 1, 2);
			jsonObject2 = rb.attackAreaLogHbase(userMap, "2014/11/15",
					"2014/11/15", appMap, devList, "power", 1, 2);
			jsonObject = rb.attackLogDetailsHbase(userMap, "2014/11/15",
					"2014/11/15", appMap, devList, 1, 2);
			jsonArray1 = rb.attackTotalHbase("0", "2014/11/15", "2014/11/15",
					"1", devList);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("%%%%%%%%%jsonArray%%%%%%");
		System.out.println(jsonArray.toString());
		System.out.println("%%%%%%%%%jsonArray%%%%%%");
		System.out.println(jsonArray1.toString());
		System.out.println("%%%%%%%%%jsonObject%%%%%%");
		System.out.println(jsonObject.toString());
		System.out.println("%%%%%%%%%jsonObject1%%%%%%");
		System.out.println(jsonObject1.toString());
		System.out.println("%%%%%%%%%jsonObject2%%%%%%");
		System.out.println(jsonObject2.toString());
	}

}
