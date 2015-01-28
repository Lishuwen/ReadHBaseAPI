package com.lsw.day0916;

//this is the new eclipse project
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

public class ReadHBaseTrafficTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table;
	private HBaseAdmin admin;

	public ReadHBaseTrafficTable(String masterIP, String masterPort,
			String zkIP, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIP);

		try {
			table = new HTable(config, Bytes.toBytes("traffdir_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	// fetch the One piece data from HBase table
	public JSONArray traffDirL2Hbase(String date, String src, String dest,
			String appType, List<String> sm_AppTypeList, String updown,
			List<String> devid) throws IOException {
		JSONArray jsonArray = new JSONArray();
		Result rst = null;

		JSONArray sm_jsonArray = new JSONArray();
		for (String value : sm_AppTypeList) {
			Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
			for (int i = 1; i < 289; i++) {
				versionMap.put(i, 0.0);
			}
			String sm_appName = null;
			for (String dev : devid) {
				// concat the rowkey with the input field
				String rowkey = date + "_" + src + "_" + dest + "_" + appType
						+ "_" + value + "_" + dev;
				System.out.println("%%%%%%%%rowkey is :"+rowkey);
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
						String[] arr2 = Bytes.toString(kv.getRow()).split("_");
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
				// jsonObject.put("appName", value);

				for (int i=1;i<=versionMap.size();i++) {
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
				String rowkey = date + "_" + src + "_" + dest + "_" + appType
						+ "_" + value + "_" + dev;
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
						String arr2[] = Bytes.toString(kv.getRow()).split("_");
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
		for (int i=1;i<=versionMap.size();i++) {
			sm_jsonArray.add(versionMap.get(i));
		}
		// jsonObject.put(appType, sm_jsonArray);
		// jsonArray.add(appTypeName);
		jsonArray.add(appType);
		jsonArray.add(sm_jsonArray);
		return jsonArray;

	}

	public static void main(String[] args) {
		ReadHBaseTrafficTable rht = new ReadHBaseTrafficTable("10.10.2.41",
				"60000", "10.10.2.41", "2181");
		JSONArray ja = new JSONArray();
		JSONArray jaSum = new JSONArray();
		List<String> l = new ArrayList<String>();
		l.add("百度手机助手");
		l.add("yourFiles");
		l.add("youtube");
		l.add("jjj");
		List<String> l2 = new ArrayList<String>();
		l2.add("48");
		l2.add("17");
		l2.add("999");
		try {
			ja = rht.traffDirL2Hbase("2014/11/20", "10", "3", "2", l,
					"AppTraffic_DN", l2);

			jaSum = rht.traffDirL2_SumHbase("2014/11/20", "10", "3", "2", l,
					"AppTraffic_DN", l2);

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("******this is the jsonarray*******");
		System.out.println(ja.toString());
		System.out.println("#########this is the Sum_jsonarray##########");
		System.out.println(jaSum.toString());
	}

}
