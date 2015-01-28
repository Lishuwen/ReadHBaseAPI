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
import org.apache.hadoop.hbase.util.Bytes;

import com.lsw.other.DayList;

public class ReadHBaseCPSPTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table,tableday;
	private HBaseAdmin admin;

	public ReadHBaseCPSPTable(String masterIP, String masterPort, String zkIp,
			String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("cpsp_5minute"));
			tableday = new HTable(config, Bytes.toBytes("cpsp_day"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	public JSONArray cpSpResL2Hbase(String date, String protType,
			Map<String, List<String>> appNames, List<String> devid)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result rst = null;

		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, List<String>> appMap : appNames.entrySet()) {
			String appName = null;
			for (String value : appMap.getValue()) {
				Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
				for (int i = 1; i < 289; i++) {
					versionMap.put(i, 0.0);
				}
				String sm_appName = null;
				for (String dev : devid) {
					// concat the rowkey with the input field
					String rowkey = date + "\001" + dev + "\001" + protType
							+ "\001" + appMap.getKey() + "\001" + value;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("HitFreq"));
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
							sm_appName = arr2[4];
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
							System.out.print("no 18 value is :"
									+ versionMap.get(18));

						}
					}
				}
				JSONArray jsonarr = new JSONArray();
				// jsonarr.add(value);
				if (sm_appName != null) {
					jsonarr.add(sm_appName);
					// jsonObject.put("appName", value);
					System.out.print("no 18 value is :" + versionMap.get(18));
					for (int i = 1; i <= versionMap.size(); i++) {
						System.out.print(i + "  " + versionMap.get(i) + "^^^");
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
	public JSONArray cpSpResL2_SumHbase(String date, String protType,
			Map<String, List<String>> appNamesMap, List<String> devNameList)
			throws IOException {

		Map<String, Map<Integer, Double>> aptimeMap = new HashMap<String, Map<Integer, Double>>();
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = null;
		// JSONObject jsonObject = new JSONObject();
		Result rst = null;
		JSONArray jsonArraysm = new JSONArray();
		for (Map.Entry<String, List<String>> appEntry : appNamesMap.entrySet()) {
			String appType = null;
			sm_jsonArray = new JSONArray();
			Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
			for (int i = 1; i < 289; i++) {
				versionMap.put(i, 0.0);
			}

			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					// concat the rowkey with the input field
					String rowkey = date + "\001" + devName + "\001" + protType
							+ "\001" + appEntry.getKey() + "\001" + appName;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("HitFreq"));
					get.setTimeRange(0L, Long.MAX_VALUE);
					rst = table.get(get);
					SimpleDateFormat sdf = new SimpleDateFormat(
							"yyyy-MM-dd hh:mm:ss");
					for (KeyValue kv : rst.raw()) {
						String arr2[] = Bytes.toString(kv.getRow()).split(
								"\001");
						appType = arr2[3];
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
			if (appType != null) {
				jsonArray.add(appType);
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

	public JSONArray cpSpResPieHbase(String date, String protType,
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
					String rowkey = date + "\001" + devName + "\001" + protType
							+ "\001" + appEntry.getKey() + "\001" + appName;
					System.out.println("rowkey" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("HitFreq"));
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

	public JSONArray cpSpResPieSUMHbase(String date, String protType,
			Map<String, List<String>> appTypeMap, List<String> devNameList)
			throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					String rowkey = date + "\001" + devName + "\001" + protType
							+ "\001" + appEntry.getKey() + "\001" + appName;
					System.out.println("rowkey:" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					// get.getMaxVersions();
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("HitFreq"));
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

	/**
	 * The part of the DAY
	 * 
	 * */
	public JSONArray cpSpResL2Hbase_day(String date1, String date2,
			String protType, Map<String, List<String>> appNames,
			List<String> devid) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");

		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray jsonArray = new JSONArray();

		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, List<String>> appMap : appNames.entrySet()) {
			String appName = null;
			for (String value : appMap.getValue()) {
				Map<String, Double> versionMap = new HashMap<String, Double>();
				for (int i = 0; i <= lDate.size()-1; i++) {
					versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
				}
				String sm_appName = null;
				for (String dev : devid) {
					List<Get> getList = new ArrayList<Get>();
					Result[] rst = null;
					for (Date day : lDate) {
						// concat the rowkey with the input field
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + dev + "\001" + protType + "\001"
								+ appMap.getKey() + "\001" + value;
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("HitFreq"));

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
								sm_appName = arr2[4];
								versionMap
										.put(arr2[0],
												versionMap.get(arr2[0])
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
								System.out.print("no 18 value is :"
										+ versionMap.get(18));
							}
						}
					}
				}
				JSONArray jsonarr = new JSONArray();
				// jsonarr.add(value);
				if (sm_appName != null) {
					jsonarr.add(sm_appName);
					// jsonObject.put("appName", value);
					System.out.print("no 18 value is :" + versionMap.get(18));
					for (int i = 0; i <= lDate.size() - 1; i++) {
						System.out.print(i + "  " + versionMap.get(i) + "^^^");
						sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i)).replace('-', '/')));
					}
					jsonarr.add(sm_jsonArray);
					sm_jsonArray.clear();
					jsonArray.add(jsonarr);
				}

			}
		}

		return jsonArray;
	}

	public JSONArray cpSpResL2_SumHbase_day(String date1, String date2,
			String protType, Map<String, List<String>> appNamesMap,
			List<String> devNameList) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		Map<String, Map<Integer, Double>> aptimeMap = new HashMap<String, Map<Integer, Double>>();
		JSONArray jsonArray = new JSONArray();
		Result[] rst = null;
		JSONArray jsonArraysm = new JSONArray();
		for (Map.Entry<String, List<String>> appEntry : appNamesMap.entrySet()) {
			String appType = null;
			JSONArray sm_jsonArray = new JSONArray();
			Map<String, Double> versionMap = new HashMap<String, Double>();
			for (int i = 0; i <= lDate.size()-1; i++) {
				versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
			}

			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						// concat the rowkey with the input field
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + devName + "\001" + protType + "\001"
								+ appEntry.getKey() + "\001" + appName;
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("HitFreq"));
						getList.add(get);
					}
					rst = tableday.get(getList);
					for (int i = 0; i < rst.length - 1; i++) {
						for (KeyValue kv : rst[i].raw()) {
							String arr2[] = Bytes.toString(kv.getRow()).split(
									"\001");
							appType = arr2[3];
							versionMap.put(
									arr2[0],
									versionMap.get(arr2[0])
											+ Double.parseDouble((Bytes
													.toString(kv.getValue()))));
							System.out.println("###this is the test output###");
							System.out
									.print(Bytes.toString(kv.getKey()) + "  ");
							System.out.print(arr2[0] + "  ");
							System.out.println(Bytes.toString(kv.getValue())
									+ "  ");
							System.out
									.println("######this is the test output########");
						}
					}
				}
			}

			if (appType != null) {
				jsonArray.add(appType);
				for (int i = 0; i <= lDate.size() - 1; i++) {
					sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i)).replace('-', '/')));
				}

				jsonArray.add(sm_jsonArray);
				jsonArraysm.add(jsonArray);
				sm_jsonArray.clear();
			}

		}

		// jsonObject.put("appName", appType);
		return jsonArray;
	}

	public JSONArray cpSpResPieHbase_day(String date1, String date2,
			String protType, Map<String, List<String>> appNames,
			List<String> devid) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray jsonArrayPie = new JSONArray();
		// System.out.println();
		for (Map.Entry<String, List<String>> appEntry : appNames.entrySet()) {
			System.out.println(appEntry.getKey());
			for (String appName : appEntry.getValue()) {
				JSONArray sm_jsonArrayPie = new JSONArray();
				System.out.println(appName);
				double sum = 0.0;
				for (String devName : devid) {
					Result[] rs = null;
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + devName + "\001" + protType + "\001"
								+ appEntry.getKey() + "\001" + appName;
						System.out.println("rowkey" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("HitFreq"));
						getList.add(get);
					}
					rs = tableday.get(getList);
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

	public JSONArray cpSpResPieSUMHbase_day(String date1, String date2,
			String protType, Map<String, List<String>> appTypeMap,
			List<String> devNameList) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String devName : devNameList) {
					Result[] rs = null;
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + devName + "\001" + protType + "\001"
								+ appEntry.getKey() + "\001" + appName;
						System.out.println("rowkey:" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes("HitFreq"));
						getList.add(get);
					}
					rs = tableday.get(getList);
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

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseCPSPTable rcpt = new ReadHBaseCPSPTable("10.10.2.41", "60000",
				"10.10.2.41", "2181");
		List<String> appList = new ArrayList<String>();
		List<String> devList = new ArrayList<String>();
		devList.add("22");
		devList.add("12");
		devList.add("16");
		Map<String, List<String>> appMap = new HashMap<String, List<String>>();
		appList.add("凤凰视频");
		appList.add("lllll");
		appMap.put("P2PStream", appList);
		appMap.put("P2PStreamwww", appList);
		JSONArray jsonArraynew = new JSONArray();
		JSONArray jsonArraynew2 = new JSONArray();
		JSONArray jsonArray = new JSONArray();
		JSONArray jsonArray1 = new JSONArray();
		try {
			jsonArraynew = rcpt.cpSpResL2Hbase_day("2014/11/19","2014/11/20", "2", appMap,
					devList);
			jsonArraynew2 = rcpt.cpSpResL2_SumHbase_day("2014/11/19","2014/11/20", "2", appMap,
					devList);
			jsonArray = rcpt
					.cpSpResPieHbase_day("2014/11/19","2014/11/20", "2", appMap, devList);
			jsonArray1 = rcpt.cpSpResPieSUMHbase_day("2014/11/19","2014/11/20", "2", appMap,
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
