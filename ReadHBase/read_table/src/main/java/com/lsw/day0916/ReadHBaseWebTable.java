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

import com.lsw.other.DayList;

public class ReadHBaseWebTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table,tableday;
	private HBaseAdmin admin;

	public ReadHBaseWebTable(String masterIP, String masterPort, String zkIp,
			String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("webdir_5minute"));
			tableday = new HTable(config, Bytes.toBytes("webdir_day"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public JSONArray webFlowL2Hbase(String date, String userGno,
			Map<String, String> siteType, String appTraffic_field,
			List<String> devid) throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result rst = null;
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, String> appMap : siteType.entrySet()) {

			Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
			for (int i = 1; i <= 288; i++) {
				versionMap.put(i, 0.0);
			}
			String sm_appName = null;
			for (String dev : devid) {

				// concat the rowkey with the input field
				String rowkey = date + "\001" + dev + "\001" + userGno + "\001"
						+ appMap.getKey();
				Get get = new Get(Bytes.toBytes(rowkey));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes(appTraffic_field));
				rst = table.get(get);
				if (rst != null) {
					for (KeyValue kv : rst.raw()) {
						System.out.println("rowkey is :"
								+ Bytes.toString(kv.getRow()));
						String[] arr2 = Bytes.toString(kv.getRow()).split(
								"\001");
						sm_appName = siteType.get(arr2[3]);
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

		return jsonArray;
	}

	public JSONArray webFlowPieHbase(String date, String userGno,
			Map<String, String> siteType, String appTraffic_field,
			List<String> devid) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();

		// System.out.println();
		for (Map.Entry<String, String> siteEntry : siteType.entrySet()) {
			System.out.println(siteEntry.getKey());

			JSONArray sm_jsonArrayPie = new JSONArray();
			System.out.println(siteEntry.getValue());
			double sum = 0.0;
			String siteName = null;
			for (String devName : devid) {
				String rowkey = date + "\001" + devName + "\001" + userGno
						+ "\001" + siteEntry.getKey();
				System.out.println("rowkey" + rowkey);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.getMaxVersions();
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes(appTraffic_field));
				get.setTimeRange(0L, Long.MAX_VALUE);
				Result rs = null;
				rs = table.get(get);

				System.out.println("PieHbase  :" + rs.toString());
				for (KeyValue kv : rs.raw()) {
					String arr[] = Bytes.toString(kv.getRow()).split("\001");
					siteName = siteType.get(arr[3]);
					sum += Double.parseDouble(Bytes.toString(kv.getValue()));
				}

				System.out.print(siteEntry.getValue() + ":" + sum + "    ");

			}
			if (siteName != null) {
				sm_jsonArrayPie.add(siteEntry.getValue());
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);
			}

		}
		return jsonArrayPie;
	}

	/**
	 * 
	 * the part of DAY
	 * 
	 * */

	public JSONArray webFlowL2Hbase_day(String date1, String date2,
			String userGno, Map<String, String> siteType,
			String appTraffic_field, List<String> devid) throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result[] rst = null;
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray sm_jsonArray = new JSONArray();
		for (Map.Entry<String, String> appMap : siteType.entrySet()) {

			Map<String, Double> versionMap = new HashMap<String, Double>();
			for (int i = 0; i <= lDate.size()-1; i++) {
				versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
			}
			String sm_appName = null;
			for (String dev : devid) {
				List<Get> getList = new ArrayList<Get>();
				for (Date day : lDate) {
					// concat the rowkey with the input field
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ dev + "\001" + userGno + "\001" + appMap.getKey();
					Get get = new Get(Bytes.toBytes(rowkey));

					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_field));
					getList.add(get);
				}
				rst = tableday.get(getList);

				if (rst != null) {
					for (int i = 0; i < rst.length - 1; i++) {
						for (KeyValue kv : rst[i].raw()) {
							System.out.println("rowkey is :"
									+ Bytes.toString(kv.getRow()));
							String[] arr2 = Bytes.toString(kv.getRow()).split(
									"\001");
							sm_appName = siteType.get(arr2[3]);

							double tmpSum = versionMap.get(arr2[0]);
							versionMap.put(
									arr2[0],
									tmpSum
											+ Double.parseDouble((Bytes
													.toString(kv.getValue()))));
							System.out.println("***this is the test output***");
							System.out
									.print(Bytes.toString(kv.getKey()) + "  ");
							System.out.print(arr2[0] + "  ");
							System.out.println(Bytes.toString(kv.getValue())
									+ "  ");
							System.out.println("***this is the test output***");
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
					sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i)).replace('-', '/')));
				}
				jsonarr.add(sm_jsonArray);
				sm_jsonArray.clear();
				jsonArray.add(jsonarr);
			}

		}

		return jsonArray;
	}

	public JSONArray webFlowPieHbase_day(String date1, String date2,
			String userGno, Map<String, String> siteType,
			String appTraffic_field, List<String> devid) throws IOException {
		JSONArray jsonArrayPie = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		// System.out.println();
		for (Map.Entry<String, String> siteEntry : siteType.entrySet()) {
			System.out.println(siteEntry.getKey());

			JSONArray sm_jsonArrayPie = new JSONArray();
			System.out.println(siteEntry.getValue());
			double sum = 0.0;
			String siteName = null;
			for (String devName : devid) {

				List<Get> getList = new ArrayList<Get>();
				for (Date day : lDate) {
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ devName + "\001" + userGno + "\001"
							+ siteEntry.getKey();
					System.out.println("rowkey" + rowkey);
					Get get = new Get(Bytes.toBytes(rowkey));

					get.addColumn(Bytes.toBytes("cf"),
							Bytes.toBytes(appTraffic_field));
					getList.add(get);
				}
				Result[] rs = tableday.get(getList);

				System.out.println("PieHbase  :" + rs.toString());
				for (int i = 0; i < rs.length - 1; i++) {
					for (KeyValue kv : rs[i].raw()) {
						String arr[] = Bytes.toString(kv.getRow())
								.split("\001");
						siteName = siteType.get(arr[3]);
						sum += Double
								.parseDouble(Bytes.toString(kv.getValue()));
						
					}
				}

				System.out.print(siteEntry.getValue() + ":" + sum + "    ");

			}
			if (siteName != null) {
				sm_jsonArrayPie.add(siteEntry.getValue());
				sm_jsonArrayPie.add(sum);
				jsonArrayPie.add(sm_jsonArrayPie);
			}

		}
		return jsonArrayPie;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseWebTable rwt = new ReadHBaseWebTable("10.10.2.41", "60000",
				"10.10.2.41", "2181");

		List<String> devList = new ArrayList<String>();
		devList.add("22");
		devList.add("23");
		devList.add("16");
		Map<String, String> siteMap = new HashMap<String, String>();
		siteMap.put("2", "海2区");
		siteMap.put("5", "Taobao");
		siteMap.put("3", "oppp");
		JSONArray jsonArraynew = new JSONArray();
		JSONArray jsonArraynew2 = new JSONArray();

		try {
			jsonArraynew = rwt.webFlowL2Hbase_day("2014/11/19","2014/11/20", "0", siteMap,
					"SiteTraffic_Up", devList);
			jsonArraynew2 = rwt.webFlowPieHbase_day("2014/11/19", "2014/11/20","0", siteMap,
					"SiteTraffic_Dn", devList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("$$$$******this is the jsonarraynew*******");
		System.out.println(jsonArraynew.toString());
		System.out.println("$$$$******this is the jsonarraynew2*******");
		System.out.println(jsonArraynew2.toString());

	}

}
