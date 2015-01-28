package com.lsw.day0916;

//this is the new eclipse project
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

import com.lsw.other.DayList;

public class ReadHBaseTrafficTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table, tableDay;
	private HBaseAdmin admin;

	public ReadHBaseTrafficTable(String masterIP, String masterPort,
			String zkIP, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIP);

		try {
			table = new HTable(config, Bytes.toBytes("traffdir_5minute"));
			tableDay = new HTable(config, Bytes.toBytes("traffdir_day"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// test function

	// fetch the One piece data from HBase table
	public JSONArray traffDirL2Hbase(String date, String src, String dest,
			String appType, List<String> sm_AppTypeList, String updown,
			List<String> devid) throws IOException {
		JSONArray jsonArray = new JSONArray();

		JSONArray sm_jsonArray = new JSONArray();
		for (String value : sm_AppTypeList) {
			Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
			for (int i = 1; i < 289; i++) {
				versionMap.put(i, 0.0);
			}
			String sm_appName = null;
			for (String dev : devid) {
				Result rst = null;
				// concat the rowkey with the input field
				String rowkey = date + "\001" + src + "\001" + dest + "\001"
						+ appType + "\001" + value + "\001" + dev;
				System.out.println("%%%%%%%%rowkey is :" + rowkey);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.getMaxVersions();
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(updown));
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
						int timeStampNum = (int) Math.ceil((hour * 3600 + min
								* 60 + sec)
								/ (60 * 5));
						double tmpSum = versionMap.get(timeStampNum);
						versionMap.put(
								timeStampNum,
								tmpSum
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));
						System.out.println("***this is the test output***");
						System.out.print(Bytes.toString(kv.getKey()) + "  ");
						System.out.print(timeStampNum + "  ");
						System.out
								.println(Bytes.toString(kv.getValue()) + "  ");
						System.out.println("***this is the test output***");
					}
				}
			}
			JSONArray jsonarr = new JSONArray();
			// jsonarr.add(value);
			if (sm_appName != null) {
				jsonarr.add(sm_appName);
				for (int i = 1; i <= versionMap.size(); i++) {
					// System.out.print(entry.getValue() + ",");
					sm_jsonArray.add(versionMap.get(i));
				}
				jsonarr.add(sm_jsonArray);
				sm_jsonArray.clear();
				jsonArray.add(jsonarr);
			}

		}

		return jsonArray;
	}

	// fetch the each piece of data from HBase table
	public JSONArray traffDirL2_SumHbase(String date, String src, String dest,
			String appType, List<String> sm_AppTypeList,
			String appTraffic_Field, List<String> devid) throws IOException {
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i < 289; i++) {
			versionMap.put(i, 0.0);
		}
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		// JSONObject jsonObject = new JSONObject();
		Result rst = null;
		String appTypeName = null;
		for (String value : sm_AppTypeList) {
			for (String dev : devid) {
				// concat the rowkey with the input field
				String rowkey = date + "\001" + src + "\001" + dest + "\001"
						+ appType + "\001" + value + "\001" + dev;
				Get get = new Get(Bytes.toBytes(rowkey));
				get.getMaxVersions();
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes(appTraffic_Field));
				get.setTimeRange(0L, Long.MAX_VALUE);
				rst = table.get(get);
				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd hh:mm:ss");
				if (rst != null) {
					for (KeyValue kv : rst.raw()) {
						String arr2[] = Bytes.toString(kv.getRow()).split(
								"\001");
						appTypeName = arr2[3];
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
		}
		// jsonObject.put("appName", appType);
		for (int i = 1; i <= versionMap.size(); i++) {
			sm_jsonArray.add(versionMap.get(i));
		}
		// jsonObject.put(appType, sm_jsonArray);
		// jsonArray.add(appTypeName);
		jsonArray.add(appType);
		jsonArray.add(sm_jsonArray);
		return jsonArray;
	}

	public JSONArray traffDirPieHbase(String date, String src, String dest,
			String appType, List<String> appNames, String appTraffic_Field,
			List<String> devid) throws IOException {
		// public JSONArray genFlowPieHbase() throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		// System.out.println();

		for (String appName : appNames) {
			JSONArray sm_jsonArrayPie = new JSONArray();
			System.out.println(appName);
			double sum = 0.0;
			String sm_appName = null;
			for (String dev : devid) {
				String rowkey = date + "\001" + src + "\001" + dest + "\001"
						+ appType + "\001" + appName + "\001" + dev;
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
								"\001");
						sm_appName = arr2[4];
						sum += Double
								.parseDouble(Bytes.toString(kv.getValue()));
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

		return jsonArrayPie;
	}

	public JSONArray traffDirPie_SumHbase(String date, String src, String dest,
			Map<String, String> appMap, Map<String, List<String>> appTypeMap,
			String appTraffic_Field, List<String> devid) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			String appType = null;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String dev : devid) {
					String rowkey = date + "\001" + src + "\001" + dest
							+ "\001" + appEntry.getKey() + "\001" + appName
							+ "\001" + dev;
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
									"\001");
							appType = arr2[3];
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					}

					System.out.print(appName + ":" + sum + "    ");
				}

			}
			if (appType != null) {
				sm_jsonArrayPie.add(appMap.get(appType));
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);

			}

		}
		return jsonArrayPie;
	}

	/**
	 * The part of day table
	 * */
	public JSONArray traffDirL2Hbase_Day(String date1, String date2,
			String src, String dest, String appType,
			List<String> sm_AppTypeList, String updown, List<String> devid)
			throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		for (String value : sm_AppTypeList) {
			Map<String, Double> versionMap = new HashMap<String, Double>();
			for (int i = 0; i <= lDate.size() - 1; i++) {
				System.out.println(sdf.format(lDate.get(i)));
				versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
			}
			String sm_appName = null;
			for (String dev : devid) {
				List<Get> getList = new ArrayList<Get>();
				Result[] rst = null;
				for (Date day : lDate) {
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ src + "\001" + dest + "\001" + appType + "\001"
							+ value + "\001" + dev;
					System.out.println("*****rowkey----->" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(updown));
					getList.add(get);
				}
				rst = tableDay.get(getList);
				System.out.println("*****rst  Length:" + rst.length);
				for (int i = 0; i < rst.length - 1; i++) {
					
					for (KeyValue kv : rst[i].raw()) {
						System.out.println("rowkey is :"
								+ Bytes.toString(kv.getRow()));
						String[] arr2 = Bytes.toString(kv.getRow()).split(
								"\001");
						sm_appName = arr2[4];
						System.out.println("Value:"+Double.parseDouble((Bytes.toString(kv
								.getValue()))));
						versionMap.put(
								arr2[0],
								versionMap.get(arr2[0])
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));
						System.out.println("***this is the test output***");
						System.out.print(Bytes.toString(kv.getKey()) + "  ");
						System.out.print(arr2[0] + "  ");
						System.out
								.println(Bytes.toString(kv.getValue()) + "  ");
						System.out.println("***this is the test output***");
					}
				}
			}
			JSONArray jsonarr = new JSONArray();
			if (sm_appName != null) {
				jsonarr.add(sm_appName);
				for (int i = 0; i <= lDate.size() - 1; i++) {
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

	// fetch the each piece of data from HBase table
	public JSONArray traffDirL2_SumHbase_Day(String date1, String date2,
			String src, String dest, String appType,
			List<String> sm_AppTypeList, String appTraffic_Field,
			List<String> devid) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		Map<String, Double> versionMap = new HashMap<String, Double>();
		for (int i = 0; i <= lDate.size() - 1; i++) {
			versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
		}
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		// JSONObject jsonObject = new JSONObject();
		Result[] rst = null;
		String appNameCurrent = null;
		for (String value : sm_AppTypeList) {
			for (String dev : devid) {
				List<Get> getList = new ArrayList<Get>();
				for (Date day : lDate) {
					// concat the rowkey with the input field
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ src + "\001" + dest + "\001" + appType + "\001"
							+ value + "\001" + dev;
					Get get = new Get(Bytes.toBytes(rowkey));
					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					getList.add(get);
				}
				rst = tableDay.get(getList);

				if (rst != null) {
					for (int i = 0; i < rst.length - 1; i++) {
						for (KeyValue kv : rst[i].raw()) {
							String arr2[] = Bytes.toString(kv.getRow()).split(
									"\001");
							appNameCurrent = arr2[3];
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
		}
		// jsonObject.put("appName", appType);
		if (appNameCurrent != null) {
			for (int i = 0; i <= lDate.size() - 1; i++) {
				sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i))
						.replace('-', '/')));
			}
			// jsonObject.put(appType, sm_jsonArray);
			// jsonArray.add(appTypeName);
			jsonArray.add(appNameCurrent);
			jsonArray.add(sm_jsonArray);
		}
		return jsonArray;
	}

	public JSONArray traffDirPieHbase_Day(String date1, String date2,
			String src, String dest, String appType, List<String> appNames,
			String appTraffic_Field, List<String> devid) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (String appName : appNames) {
			JSONArray sm_jsonArrayPie = new JSONArray();
			System.out.println(appName);
			double sum = 0.0;
			String sm_appName = null;
			for (String dev : devid) {
				Result[] rs = null;
				List<Get> getList = new ArrayList<Get>();
				for (Date day : lDate) {
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ src + "\001" + dest + "\001" + appType + "\001"
							+ appName + "\001" + dev;
					System.out.println("rowkey" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));

					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_Field));
					getList.add(get);
				}
				rs = tableDay.get(getList);

				System.out.println("PieHbase  :" + rs.toString());
				for (int i = 0; i < rs.length - 1; i++) {
					for (KeyValue kv : rs[i].raw()) {
						String[] arr2 = Bytes.toString(kv.getRow()).split(
								"\001");
						sm_appName = arr2[4];
						sum += Double
								.parseDouble(Bytes.toString(kv.getValue()));
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

		return jsonArrayPie;
	}

	public JSONArray traffDirPie_SumHbase_Day(String date1, String date2,
			String src, String dest, Map<String, String> appMap,
			Map<String, List<String>> appTypeMap, String appTraffic_Field,
			List<String> devid) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (Map.Entry<String, List<String>> appEntry : appTypeMap.entrySet()) {
			double sum = 0.0;
			String appType = null;
			JSONArray sm_jsonArrayPie = new JSONArray();
			for (String appName : appEntry.getValue()) {
				for (String dev : devid) {
					Result[] rs = null;
					List<Get> getList = new ArrayList<Get>();
					for (Date day : lDate) {
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + src + "\001" + dest + "\001"
								+ appEntry.getKey() + "\001" + appName + "\001"
								+ dev;
						System.out.println("SumPierowkey:" + rowkey);
						Get get = new Get(Bytes.toBytes(rowkey));
						// get.getMaxVersions();
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(appTraffic_Field));
						// get.setTimeRange(0L, Long.MAX_VALUE);
						getList.add(get);
					}
					rs = tableDay.get(getList);

					for (int i = 0; i < rs.length - 1; i++) {
						for (KeyValue kv : rs[i].raw()) {
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"\001");
							
							appType = arr2[3];
							sum += Double.parseDouble(Bytes.toString(kv
									.getValue()));
						}
					}

					System.out.print(appType + ":" + sum + "    ");

				}
			}
			if (appType != null) {
				sm_jsonArrayPie.add(appMap.get(appType));
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);

			}

		}
		return jsonArrayPie;
	}

	public static void main(String[] args) throws IOException {
		ReadHBaseTrafficTable rht = new ReadHBaseTrafficTable("10.10.2.41",
				"60000", "10.10.2.41", "2181");
		JSONArray ja = new JSONArray();
		JSONArray jaSum = new JSONArray();
		JSONArray jaPie = new JSONArray();
		JSONArray jaSumPie = new JSONArray();
		List<String> l = new ArrayList<String>();
		Map<String, List<String>> appMap = new HashMap<String, List<String>>();
		Map<String, String> appMapKv = new HashMap<String, String>();
//		l.add("百度手机助手");
//		l.add("yourFiles");
//		l.add("youtube");
//		l.add("jjj");
		l.add("webICQ");
		l.add("SMTP");
		l.add("xunlei");
		l.add("风行HTTP流");
		appMap.put("1", l);
	//	appMap.put("7", l);
		appMap.put("2", l);
		appMapKv.put("8", "lsw");
		appMapKv.put("2", "sw");

		List<String> l2 = new ArrayList<String>();
//		l2.add("48");
//		l2.add("17");
//		l2.add("999");
//		l2.add("22");
		l2.add("65");
		l2.add("11");
		l2.add("devname");
		try {
			ja = rht.traffDirL2Hbase_Day("2014/11/01", "2014/11/30", "1", "2",
					"2", l, "AppTraffic_DN", l2);
			//
			jaSum = rht.traffDirL2_SumHbase_Day("2014/11/01", "2014/11/30",
					"1", "2", "2", l, "AppTraffic_DN", l2);
			jaPie = rht.traffDirPieHbase_Day("2014/11/01", "2014/11/30", "1",
					"2", "2", l, "AppTraffic_DN", l2);
			jaSumPie = rht.traffDirPie_SumHbase_Day("2014/11/01", "2014/11/30",
					"1", "2", appMapKv, appMap, "AppTraffic_DN", l2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("******this is the jsonarray*******");
		System.out.println(ja.toString());
		System.out.println("#########this is the Sum_jsonarray##########");
		System.out.println(jaSum.toString());
		System.out.println("******this is the jsonarrayPie*******");
		System.out.println(jaPie.toString());
		System.out.println("#########this is the Sum_jsonarrayPie##########");
		System.out.println(jaSumPie.toString());
		// Scan scan = new Scan();
		//
		// scan.setStartRow(Bytes.toBytes("2014/11/20_10_3_2_Webdisk_22"));
		// scan.setStopRow(Bytes.toBytes("2014/11/20_10_3_2_Webdisk_70"));
		// scan.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AppTraffic_DN"));
		// ResultScanner rs = rht.table.getScanner(scan);
		// for (Result r : rs) {
		// System.out.println(r.size());
		// for (KeyValue kv : r.raw()) {
		// // if(Bytes.toString(kv.getValue()).indexOf("仙剑奇侠传")!=-1){
		// System.out.print(Bytes.toString(kv.getRow()) + "  ");
		// System.out.print(new String(kv.getValue()) + "  ");
		// // System.out.println();
		// }
		// System.out.println();
		// // }
		// }
		// }
	}
}
