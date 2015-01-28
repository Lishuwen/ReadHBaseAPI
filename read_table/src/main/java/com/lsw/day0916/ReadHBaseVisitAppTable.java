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

import com.lsw.other.DayList;

public class ReadHBaseVisitAppTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table, tableday;
	private HBaseAdmin admin;

	public ReadHBaseVisitAppTable(String masterIP, String masterPort,
			String zkIp, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("nameduser_5minute"));
			tableday = new HTable(config, Bytes.toBytes("nameduser_day"));
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
					String rowkey = date + "\001" + dev + "\001"
							+ appMap.getKey() + "\001" + value;
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
									"\001");
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
					String rowkey = date + "\001" + devName + "\001"
							+ appEntry.getKey() + "\001" + appName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("UserNum"));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					for (KeyValue kv : rst.raw()) {
						String arr2[] = Bytes.toString(kv.getRow()).split(
								"\001");
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
					String rowkey = date + "\001" + devName + "\001"
							+ appEntry.getKey() + "\001" + appName;
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
					String rowkey = date + "\001" + devName + "\001"
							+ appEntry.getKey() + "\001" + appName;
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

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject visitAppLog(String date1, String date2,
			Map<String, List<String>> appTypeMap, Map<String, String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		// JSONObject jsonObject=new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		int total = 0;
		DayList dy = new DayList();
		List<Date> lDate = dy.findDates(date1, date2);
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		String[] column = { "R_StartTime", "AppNameLength", "UserNum" };
		Result[] result = new Result[column.length];
		for (Map.Entry<String, List<String>> appType : appTypeMap.entrySet())
			for (String app : appType.getValue()) {
				for (Map.Entry<String, String> dev : devName.entrySet())
					for (Date day : lDate) {
						JSONArray propertyArray = new JSONArray();
						System.out.println("....." + devName);
						String rowkey = sdf2.format(day).replace('-', '/')
								+ "\001" + dev.getKey() + "\001" + appType.getKey()
								+ "\001" + app;
						System.out.println("rowkey" + rowkey);
						for (int no = 0; no <= column.length - 1; no++) {
							System.out.println("column " + column[no]);
							Get get = new Get(Bytes.toBytes(rowkey));
							get.setMaxVersions();
							get.setTimeRange(0L, Long.MAX_VALUE);
							get.addColumn(Bytes.toBytes("cf"),
									Bytes.toBytes(column[no]));
							result[no] = table.get(get);
							//System.out.println("resultlength" + result.length);
						}
					//	System.out.println("result[1] row" + Bytes.toString(result[1].getRow()));

						if (result[1] .getRow()!= null) {
							System.out.println("&&&&&&&&&&&&&");
							System.out.println("resultlength" + result.length);
							total = result[1].size();
							int realLength=iDisplayLength+iDisplayStart;
							if(realLength>=result[1].size()){
								realLength=result[1].size();
							}
							for (int kno = iDisplayStart; kno <=realLength-1; kno++) {								Map<Integer, String> resultMap = new HashMap<Integer, String>();
								propertyArray.add(dev.getValue());
								localRows++;
								for (int i = 0; i <= result.length - 1; i++) {
									System.out.println(result[i].list().get(kno)
									.getTimestamp());
									System.out.println(Bytes.toString(result[2].list()
											.get(kno).getValue()));

									String[] rowkeyarr = Bytes.toString(
											result[i].getRow()).split("\001");
									System.out.println(rowkeyarr[3]);
									resultMap.put(
											1,
											sdf.format(result[i].list().get(kno)
													.getTimestamp()).replace('-', '/'));
								if (i < 1) {
										
										resultMap.put(
												i,
												Bytes.toString(result[i].list()
														.get(kno).getValue()));
									}
									if (i == 1) {
										resultMap.put(2, rowkeyarr[2]);
										resultMap.put(
												i+2,
												Bytes.toString(result[i].list()
														.get(kno).getValue()));

									}	
										resultMap.put(4, rowkeyarr[3]);

									
									resultMap.put(
											5,
											Bytes.toString(result[2].list()
													.get(kno).getValue()));
								//	resultMap.put(6, rowkeyarr[3]);
									System.out.println("##resultMap:" + i
											+ "-->" + resultMap.get(i));
								}
								for (int mno = 0; mno <= resultMap.size()-1; mno++) {
									System.out.println("*****resultMap:" + mno
											+ "-->" + resultMap.get(mno));
									propertyArray.add(resultMap.get(mno));
								}

								outArray.add(propertyArray);
								propertyArray.clear();

							}
						}

					}
			}

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", localRows);
		allObject.put("iTotalRecords", total);
		return allObject;
	}

	/**
	 * the part of DAY
	 * 
	 * */

	public JSONArray VisitAppUserTrdHbase_day(String date1, String date2,
			Map<String, List<String>> appNames, List<String> devid)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result[] rst = null;
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, List<String>> appMap : appNames.entrySet()) {

			for (String value : appMap.getValue()) {
				Map<String, Double> versionMap = new HashMap<String, Double>();
				for (int i = 0; i <= lDate.size() - 1; i++) {
					versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'),
							0.0);
				}
				String sm_appName = null;
				for (String dev : devid) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						// concat the rowkey with the input field
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + dev + "\001" + appMap.getKey()
								+ "\001" + value;
						Get get = new Get(Bytes.toBytes(rowkey));

						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("UserNum"));
						getList.add(get);
					}
					rst = tableday.get(getList);

					if (rst != null) {
						for (int i = 0; i < rst.length - 1; i++) {
							for (KeyValue kv : rst[i].raw()) {
								System.out.println("rowkey is :"
										+ Bytes.toString(kv.getRow()));
								String[] arr2 = Bytes.toString(kv.getRow())
										.split("\001");
								sm_appName = arr2[3];

								double tmpSum = versionMap.get(arr2[0]);
								versionMap
										.put(arr2[0],
												tmpSum
														+ Double.parseDouble((Bytes.toString(kv
																.getValue()))));
								System.out
										.println("***this is the test output***");
								System.out.print(Bytes.toString(kv.getKey())
										+ "  ");
								System.out.print(arr2[0] + "  ");
								System.out
										.println(Bytes.toString(kv.getValue())
												+ "  ");
								System.out
										.println("***this is the test output***");
							}
						}
					}
				}
				JSONArray jsonarr = new JSONArray();
				// jsonarr.add(value);
				if (sm_appName != null) {
					jsonarr.add(sm_appName);
					// jsonObject.put("appName", value);

					for (int i = 0; i <= lDate.size() - 1; i++) {
						// System.out.print(entry.getValue() + ",");
						sm_jsonArray.add(versionMap.get(sdf
								.format(lDate.get(i)).replace('-', '/')));
					}
					jsonarr.add(sm_jsonArray);
					sm_jsonArray.clear();
					jsonArray.add(jsonarr);
				}

			}
		}

		return jsonArray;
	}

	public JSONArray visitAppUserTrd_SumHbase_day(String date1, String date2,
			Map<String, List<String>> appNamesMap, List<String> devNameList)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result[] rst = null;
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, List<String>> appMap : appNamesMap.entrySet()) {
			String sm_appName = null;
			Map<String, Double> versionMap = new HashMap<String, Double>();
			for (String value : appMap.getValue()) {
				for (int i = 0; i <= lDate.size() - 1; i++) {
					versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'),
							0.0);
				}
				for (String dev : devNameList) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						// concat the rowkey with the input field
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + dev + "\001" + appMap.getKey()
								+ "\001" + value;
						Get get = new Get(Bytes.toBytes(rowkey));

						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("UserNum"));
						getList.add(get);
					}
					rst = tableday.get(getList);

					if (rst != null) {
						for (int i = 0; i < rst.length - 1; i++) {
							for (KeyValue kv : rst[i].raw()) {
								System.out.println("rowkey is :"
										+ Bytes.toString(kv.getRow()));
								String[] arr2 = Bytes.toString(kv.getRow())
										.split("\001");
								sm_appName = arr2[2];

								double tmpSum = versionMap.get(arr2[0]);
								versionMap
										.put(arr2[0],
												tmpSum
														+ Double.parseDouble((Bytes.toString(kv
																.getValue()))));
								System.out
										.println("***this is the test output***");
								System.out.print(Bytes.toString(kv.getKey())
										+ "  ");
								System.out.print(arr2[0] + "  ");
								System.out
										.println(Bytes.toString(kv.getValue())
												+ "  ");
								System.out
										.println("***this is the test output***");
							}
						}
					}
				}

			}
			JSONArray jsonarr = new JSONArray();
			// jsonarr.add(value);
			if (sm_appName != null) {
				jsonarr.add(sm_appName);
				// jsonObject.put("appName", value);

				for (int i = 0; i <= lDate.size() - 1; i++) {
					// System.out.print(entry.getValue() + ",");
					sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i))
							.replace('-', '/')));
				}
				jsonarr.add(sm_jsonArray);
				sm_jsonArray.clear();
				jsonArray.add(jsonarr);
			}

		}

		return jsonArray;
	}

	public JSONArray visitAppUserPieHbase_day(String date1, String date2,
			Map<String, List<String>> appNames, List<String> devid)
			throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		// System.out.println();
		for (Map.Entry<String, List<String>> appEntry : appNames.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray sm_jsonArrayPie = new JSONArray();
				System.out.println(appName);
				double sum = 0.0;
				for (String devName : devid) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + devName + "\001" + appEntry.getKey()
								+ "\001" + appName;
						System.out.println("rowkey" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("UserNum"));
						getList.add(get);
					}
					Result[] rs = tableday.get(getList);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (int i = 0; i < rs.length - 1; i++) {
							for (KeyValue kv : rs[i].raw()) {
								sum += Double.parseDouble(Bytes.toString(kv
										.getValue()));
							}
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

	public JSONArray visitAppUserPie_SUMHbase_day(String date1, String date2,
			Map<String, List<String>> appTypeMap, List<String> devNameList)
			throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + devName + "\001" + appEntry.getKey()
								+ "\001" + appName;
						System.out.println("rowkey:" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("UserNum"));
						getList.add(get);
					}
					Result[] rs = tableday.get(getList);
					if (rs != null) {
						System.out.println("PieHbase  :" + rs.toString());
						for (int i = 0; i < rs.length - 1; i++) {
							for (KeyValue kv : rs[i].raw()) {
								sum += Double.parseDouble(Bytes.toString(kv
										.getValue()));
							}
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

	// public JSONObject vistAppLog_day(String date1, String date2,
	// Map<String, List<String>> appTypeMap, Map<String, String> devName,
	// int iDisplayStart, int iDisplayLength) throws IOException {
	// // JSONObject jsonObject=new JSONObject();
	// DayList dy = new DayList();
	// SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
	// SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
	// List<Date> lDate = dy.findDates(date1, date2);
	// Filter filter = new PageFilter(iDisplayLength);
	// int localRows = iDisplayStart;
	// int total = 0;
	// byte[] lastRow = Bytes.toBytes(iDisplayStart);
	// System.out.println(lastRow.length);
	// JSONArray outArray = new JSONArray();
	// JSONObject allObject = new JSONObject();
	//
	// for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
	// // resultMap.put(5, appEntry.getKey());
	// for (String appName : appEntry.getValue()) {
	// // resultMap.put(6, appName);
	// for (Map.Entry<String, String> devEntry : devName.entrySet()) {
	//
	// List<Get> getList = new ArrayList<Get>();
	// for (Date day : lDate) {
	// Map<Integer, String> resultMap = new HashMap<Integer, String>();
	// JSONArray propertyArray = new JSONArray();
	//
	// System.out.println("....." + devName);
	// String rowkey = sdf.format(day).replace('-', '/')
	// + "\001" + devEntry.getKey() + "\001"
	// + appEntry.getKey() + "\001" + appName;
	// System.out.println("rowkey" + rowkey);
	// Get get = new Get(Bytes.toBytes(rowkey));
	// get.addColumn(Bytes.toBytes("cf"),
	// Bytes.toBytes("R_StartTime"));
	// get.addColumn(Bytes.toBytes("cf"),
	// Bytes.toBytes("AppNameLength"));
	// get.addColumn(Bytes.toBytes("cf"),
	// Bytes.toBytes("UserNum"));
	// // scan.setStartRow(StartRow);
	//
	//
	// Result result = tableday.get(get);
	// System.out.println("Log Detail");
	// System.out.println(result.toString());
	//
	// if (result.list() != null) {
	// int index = 1;
	// for (KeyValue kv : result.list()) {
	//
	// System.out.print(index + "\001");
	// System.out.println(localRows++ + ":" + kv);
	//
	// String[] arr = Bytes.toString(kv.getRow())
	// .split("\001");
	// if (index <= 8) {
	// resultMap.put(1, devName.get(arr[1]));
	// if (Bytes.toString(kv.getQualifier())
	// .equals("R_StartTime")) {
	// resultMap.put(2,
	// Bytes.toString(kv.getValue()));
	// }
	// resultMap
	// .put(3,
	// sdf2.format(
	// new Date(
	// kv.getTimestamp()))
	// .replace('-', '/'));
	// resultMap.put(4, arr[2]);
	//
	// if (Bytes.toString(kv.getQualifier())
	// .equals("AppNameLength")) {
	// resultMap.put(5,
	// Bytes.toString(kv.getValue()));
	// }
	// resultMap.put(6, arr[3]);
	// if (Bytes.toString(kv.getQualifier())
	// .equals("UserNum")) {
	// resultMap.put(7,
	// Bytes.toString(kv.getValue()));
	// }
	// }
	// if (index == 3) {
	// total++;
	// for (int j = 1; j <= resultMap.size(); j++) {
	// System.out.print("^^^^ "
	// + resultMap.get(j));
	// propertyArray.add(resultMap.get(j));
	// }
	// outArray.add(propertyArray);
	//
	// index = 0;
	// resultMap.clear();
	// propertyArray.clear();
	// }
	// index++;
	//
	//
	// }
	// }
	// }
	//
	// }
	// }
	// }
	// allObject.put("aaData", outArray);
	// allObject.put("iTotalDisplayRecords", total);
	// allObject.put("iTotalRecords", total);
	// return allObject;
	// }

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseVisitAppTable rvat = new ReadHBaseVisitAppTable("10.10.2.41",
				"60000", "10.10.2.41", "2181");
		List<String> appList = new ArrayList<String>();
		List<String> devList = new ArrayList<String>();
		devList.add("22");
		devList.add("10");
		devList.add("16");
		Map<String, List<String>> appMap = new HashMap<String, List<String>>();
		Map<String, String> devMap = new HashMap<String, String>();
		devMap.put("16", "dev16");
		devMap.put("17", "dev17");
		appList.add("POCO");
		appList.add("风行HTTP流");
		appMap.put("Web视频类", appList);
		JSONArray jsonArraynew = new JSONArray();
		JSONArray jsonArraynew2 = new JSONArray();
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray1 = new JSONArray();
		JSONObject jsonObject = new JSONObject();
		try {
			 jsonArraynew =
			 rvat.VisitAppUserTrdHbase_day("2014/11/19","2014/11/20", appMap,
			 devList);
			jsonArraynew2 = rvat.visitAppUserTrd_SumHbase_day("2014/11/19",
					"2014/11/20", appMap, devList);
			jsonArray = rvat.visitAppUserPieHbase_day("2014/11/19",
					"2014/11/20", appMap, devList);
			jsonArray1 = rvat.visitAppUserPie_SUMHbase_day("2014/11/19",
					"2014/11/20", appMap, devList);
			jsonObject = rvat.visitAppLog("2014/11/19", "2014/11/20", appMap,
					devMap, 0, 10);
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
		System.out.println("$$$$******this is the jsonObject*******");
		System.out.println(jsonObject.toString());

	}

}
