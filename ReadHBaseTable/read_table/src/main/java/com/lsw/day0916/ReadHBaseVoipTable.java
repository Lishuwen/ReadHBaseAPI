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

public class ReadHBaseVoipTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table;
	private HBaseAdmin admin;

	public ReadHBaseVoipTable(String masterIp, String masterPort, String zkIp,
			String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIp + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("voip_5minute"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public JSONArray voipFlowTrdHbase(String date, String userGroup,
			List<String> protName, List<String> devName) throws IOException {
		JSONArray jsonArray = new JSONArray();
		for (String prot : protName) {
			Map<Integer, Double> protMap = new HashMap<Integer, Double>();
			for (int i = 1; i < 289; i++) {
				protMap.put(i, 0.0);
			}
			JSONArray sm_jsonArray = new JSONArray();
			for (String dev : devName) {
				String rowkey = dev + "_" + date + "_" + userGroup;
				System.out.println("------rowkey :" + rowkey);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(prot));
				Result rs = table.get(get);
				SimpleDateFormat sdf = new SimpleDateFormat(
						"yyyy-MM-dd hh:mm:ss");
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
					if (protMap.get(timeStampNum) != null) {
						tmpSum = protMap.get(timeStampNum);
					}
					protMap.put(
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
			jsonarr.add(prot);
			for (Map.Entry<Integer, Double> entry : protMap.entrySet()) {
				// System.out.print(entry.getValue() + ",");
				sm_jsonArray.add(entry.getValue());
			}
			jsonarr.add(sm_jsonArray);
			jsonArray.add(jsonarr);

		}

		return jsonArray;
	}

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject voipFlowLogHbase(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
			JSONArray m_jsonArray = new JSONArray();
			for (String dev : devName) {
				Map<Integer, String> resultMap = new HashMap<Integer, String>();
				String rowkey = dev + "_" + date1 + "_" + userEntry.getKey();
				Get get = new Get(Bytes.toBytes(rowkey));
				get.setMaxVersions();
				get.setTimeRange(0L, Long.MAX_VALUE);
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("RTP"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("其它"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("H.323"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SIP"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("MGCP"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWNum"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWKeeperNum"));
				get.setFilter(filter);
				Result rs = null;
				rs = table.get(get);
				if (lastRow != null) {
					byte[] StartRow = Bytes.add(lastRow, POSTFIX);
					System.out.println("start row:" + Bytes.toInt(StartRow));

					// scan.setStartRow(StartRow);
				}
				// ResultScanner scanner = table.getScanner(scan);
				int localRows = 1;
				int index = 1;

				JSONArray sm_jsonArray = new JSONArray();
				if (rs.list() != null) {
					for (KeyValue kv : rs.list()) {

						String[] arr = Bytes.toString(kv.getRow()).split("_");
						if (index <= 9) {
							resultMap.put(1, dev);
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_StartTime")) {
								resultMap.put(2, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_EndTime")) {
								resultMap.put(3, Bytes.toString(kv.getValue()));
							}
							resultMap.put(4, userGroup.get(arr[2]));
							if (Bytes.toString(kv.getQualifier()).equals("RTP")) {
								resultMap.put(5, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals("其它")) {
								resultMap.put(6, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"H.323")) {
								resultMap.put(7, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals("SIP")) {
								resultMap.put(8, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier())
									.equals("MGCP")) {
								resultMap.put(9, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"GWNum")) {
								System.out.println("^^^^^^map 10^^^^^^^^^");
								resultMap
										.put(10, Bytes.toString(kv.getValue()));
								System.out.println("^^^^^^map 10:"
										+ resultMap.get(10));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"GWKeeperNum")) {
								resultMap
										.put(11, Bytes.toString(kv.getValue()));
								System.out.println("^^^^^^map 11:"
										+ resultMap.get(11));
							}
						}
						for (int i = 1; i < resultMap.size(); i++) {
							System.out.println("@@@" + i + " :"
									+ resultMap.get(i));
						}
						if (index == 9) {
							totalRows++;
							for (int i = 1; i <= resultMap.size(); i++) {
								sm_jsonArray.add(resultMap.get(i));
							}
							m_jsonArray.add(sm_jsonArray);
							resultMap.clear();
							sm_jsonArray.clear();
							index = 0;
						}
						index++;
					}
				}
			}
			jsonObject.put("aaData", m_jsonArray);
			jsonObject.put("iTotalDisplayRecords", totalRows);
			jsonObject.put("iTotalRecords", totalRows);
		}

		return jsonObject;
	}

	public JSONObject voipGWLogHbase(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
			JSONArray m_jsonArray = new JSONArray();
			for (String dev : devName) {
				String rowkey = dev + "_" + date1 + "_" + userEntry.getKey();
				Get get = new Get(Bytes.toBytes(rowkey));
				get.setMaxVersions();
				get.setTimeRange(0L, Long.MAX_VALUE);
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GW_IPLength"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GW_IP"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("TotalCallSessions"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CallSessionsConcurrent"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("TotalCallDurations"));
				get.setFilter(filter);
				Result rs = table.get(get);
				if (lastRow != null) {
					byte[] StartRow = Bytes.add(lastRow, POSTFIX);
					System.out.println("start row:" + Bytes.toInt(StartRow));
					// scan.setStartRow(StartRow);
				}
				int index = 1;
				Map<Integer, String> rMap = new HashMap<Integer, String>();
				JSONArray sm_jsonArray = new JSONArray();
				if (rs.list() != null) {
					for (KeyValue kv : rs.list()) {
						String[] arr = Bytes.toString(kv.getRow()).split("_");
						if (index <= 7) {
							rMap.put(1, arr[0]);
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_StartTime")) {
								rMap.put(2, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_EndTime")) {
								rMap.put(3, Bytes.toString(kv.getValue()));
							}
							rMap.put(4, userGroup.get(arr[2]));
							if (Bytes.toString(kv.getQualifier()).equals(
									"GW_IPLength")) {
								rMap.put(5, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"GW_IP")) {
								rMap.put(6, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"TotalCallSessions")) {
								rMap.put(7, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CallSessionsConcurrent")) {
								rMap.put(8, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"TotalCallDurations")) {
								rMap.put(9, Bytes.toString(kv.getValue()));
							}
						}
						if (index == 7) {
							total++;
							for (int i=1;i<=rMap.size();i++) {
								sm_jsonArray.add(rMap.get(i));
							}
							m_jsonArray.add(sm_jsonArray);
							sm_jsonArray.clear();
							rMap.clear();
							index = 0;
						}
						index++;
					}
				}
				jsonObject.put("aaData", m_jsonArray);
				jsonObject.put("iTotalDisplayRecords", total);
				jsonObject.put("iTotalRecords", total);
			}
		}
		return jsonObject;
	}

	public JSONObject voipCalledLogHbase(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
			JSONArray m_jsonArray = new JSONArray();

			for (String dev : devName) {
				Map<Integer, String> gwMap = new HashMap<Integer, String>();
				JSONArray sm_jsonArray = new JSONArray();
				String rowkey = dev + "_" + date1 + "_" + userEntry.getKey();
				Get get = new Get(Bytes.toBytes(rowkey));
				get.setMaxVersions();
				get.setTimeRange(0L, Long.MAX_VALUE);
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("GWKeeper_IPLength"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWKeeper_IP"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CalledNumber_Length"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CalledNumber"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CalledDuration"));
				get.setFilter(filter);
				Result rs = table.get(get);
				int index = 1;
				if (rs.list() != null) {
					for (KeyValue kv : rs.list()) {
						String[] arr = Bytes.toString(kv.getRow()).split("_");
						if (index <= 7) {
							gwMap.put(1, arr[0]);
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_StartTime")) {
								gwMap.put(2, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_EndTime")) {
								gwMap.put(3, Bytes.toString(kv.getValue()));
							}
							gwMap.put(4, userGroup.get(arr[2]));
							if (Bytes.toString(kv.getQualifier()).equals(
									"GWKeeper_IPLength")) {
								gwMap.put(5, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"GWKeeper_IP")) {
								gwMap.put(6, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CalledNumber_Length")) {
								gwMap.put(7, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CalledNumber")) {
								gwMap.put(8, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CalledDuration")) {
								gwMap.put(9, Bytes.toString(kv.getValue()));
							}
						}
						if (index == 7) {
							total++;
							for (Map.Entry<Integer, String> gwEntry : gwMap
									.entrySet()) {
								sm_jsonArray.add(gwEntry.getValue());
							}
							m_jsonArray.add(sm_jsonArray);
							sm_jsonArray.clear();
							gwMap.clear();
							index = 0;
						}
						index++;
					}
				}
			}
			jsonObject.put("aaData", m_jsonArray);
			jsonObject.put("iTotalDispalyRecords", total);
			jsonObject.put("iTotalRecords", total);
		}
		return jsonObject;

	}

	public JSONObject voipCallerLogHbase(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int totalRows = 0;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
			JSONArray m_jsonArray = new JSONArray();
			for (String dev : devName) {
				Map<Integer, String> gwMap = new HashMap<Integer, String>();
				JSONArray sm_jsonArray = new JSONArray();
				String rowkey = dev + "_" + date1 + "_" + userEntry.getKey();
				Get get = new Get(Bytes.toBytes(rowkey));
				get.setMaxVersions();
				get.setTimeRange(0L, Long.MAX_VALUE);
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("GWKeeper_IPLength"));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWKeeper_IP"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CallerNumber_Length"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CallerNumber"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("CallerDuration"));
				get.setFilter(filter);
				Result rs = table.get(get);
				int index = 1;
				if (rs.list() != null) {
					for (KeyValue kv : rs.list()) {
						String[] arr = Bytes.toString(kv.getRow()).split("_");
						if (index <= 7) {
							gwMap.put(1, arr[0]);
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_StartTime")) {
								gwMap.put(2, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"R_EndTime")) {
								gwMap.put(3, Bytes.toString(kv.getValue()));
							}
							gwMap.put(4, userGroup.get(arr[2]));
							if (Bytes.toString(kv.getQualifier()).equals(
									"GWKeeper_IPLength")) {
								gwMap.put(5, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"GWKeeper_IP")) {
								gwMap.put(6, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CallerNumber_Length")) {
								gwMap.put(7, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CallerNumber")) {
								gwMap.put(8, Bytes.toString(kv.getValue()));
							}
							if (Bytes.toString(kv.getQualifier()).equals(
									"CallerDuration")) {
								gwMap.put(9, Bytes.toString(kv.getValue()));
							}
						}
						if (index == 7) {
							total++;
							for (Map.Entry<Integer, String> gwEntry : gwMap
									.entrySet()) {
								sm_jsonArray.add(gwEntry.getValue());
							}
							m_jsonArray.add(sm_jsonArray);
							sm_jsonArray.clear();
							gwMap.clear();
							index = 0;
						}
						index++;
					}
				}
			}
			jsonObject.put("aaData", m_jsonArray);
			jsonObject.put("iTotalDispalyRecords", total);
			jsonObject.put("iTotalRecords", total);
		}
		return jsonObject;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseVoipTable rbv = new ReadHBaseVoipTable("10.10.2.41", "60000",
				"10.10.2.41", "2181");
		String []userStr=args[0].split("_");
		String []devStr=args[1].split("_");
		Map<String, String> userMap = new HashMap<String, String>();
		userMap.put(userStr[0], userStr[1]);
		userMap.put("0", "哈哈哈");
		List<String> devList = new ArrayList<String>();
		devList.add(devStr[0]);
		devList.add(devStr[1]);
		devList.add("测试");
		devList.add("pp");
		List<String> protList = new ArrayList<String>();
//		protList.add("SIP");
//		protList.add("MGCP");
		protList.add("RTP");
		JSONArray jsonArray = new JSONArray();
		JSONObject jsonObject = new JSONObject();
		JSONObject jsonObject1 = new JSONObject();
		JSONObject jsonObject2 = new JSONObject();
		JSONObject jsonObject3 = new JSONObject();
		try {
			jsonArray = rbv.voipFlowTrdHbase("2014/11/18", "0", protList,
					devList);
			jsonObject1 = rbv.voipGWLogHbase("2014/11/18", "2014/11/18",
					userMap, devList, 1, 2);
			jsonObject = rbv.voipFlowLogHbase(args[2], "2014/11/18",
					userMap, devList, 1, 2);
			jsonObject2 = rbv.voipCalledLogHbase("2014/11/18", "2014/11/18",
					userMap, devList, 1, 2);
			jsonObject3 = rbv.voipCallerLogHbase("2014/11/18", "2014/11/18",
					userMap, devList, 1, 2);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("****jsonArray****");
		System.out.println(jsonArray.toString());
		System.out.println("****jsonObject****");
		System.out.println(jsonObject.toString());
		System.out.println("****jsonObject1****");
		System.out.println(jsonObject1.toString());
		System.out.println("****jsonObject2****");
		System.out.println(jsonObject2.toString());
		System.out.println("****jsonObject3****");
		System.out.println(jsonObject3.toString());
	}

}
