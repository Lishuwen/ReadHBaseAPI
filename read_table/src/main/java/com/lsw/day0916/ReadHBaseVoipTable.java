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

public class ReadHBaseVoipTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table, tableDay;
	private HBaseAdmin admin;

	public ReadHBaseVoipTable(String masterIp, String masterPort, String zkIp,
			String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIp + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("voip_5minute"));
			tableDay = new HTable(config, Bytes.toBytes("voip_day"));
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
				String rowkey = dev + "\001" + date + "\001" + userGroup;
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

//	public JSONObject voipFlowLogHbase(String date1, String date2,
//			Map<String, String> userGroup, List<String> devName,
//			int iDisplayStart, int iDisplayLength) throws IOException {
//		JSONObject jsonObject = new JSONObject();
//		Filter filter = new PageFilter(iDisplayLength);
//		int totalRows = 0;
//		int total = 0;
//		byte[] lastRow = Bytes.toBytes(iDisplayStart);
//		System.out.println(lastRow.length);
//		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
//			JSONArray m_jsonArray = new JSONArray();
//			for (String dev : devName) {
//				Map<Integer, String> resultMap = new HashMap<Integer, String>();
//				String rowkey = dev + "\001" + date1 + "\001"
//						+ userEntry.getKey();
//				Get get = new Get(Bytes.toBytes(rowkey));
//				get.setMaxVersions();
//				get.setTimeRange(0L, Long.MAX_VALUE);
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("RTP"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("其它"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("H.323"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("SIP"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("MGCP"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWNum"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWKeeperNum"));
//				get.setFilter(filter);
//				Result rs = null;
//				rs = table.get(get);
//				if (lastRow != null) {
//					byte[] StartRow = Bytes.add(lastRow, POSTFIX);
//					System.out.println("start row:" + Bytes.toInt(StartRow));
//
//					// scan.setStartRow(StartRow);
//				}
//				// ResultScanner scanner = table.getScanner(scan);
//				int localRows = 1;
//				int index = 1;
//
//				JSONArray sm_jsonArray = new JSONArray();
//				if (rs.list() != null) {
//					for (KeyValue kv : rs.list()) {
//
//						String[] arr = Bytes.toString(kv.getRow())
//								.split("\001");
//						if (index <= 9) {
//							resultMap.put(1, dev);
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_StartTime")) {
//								resultMap.put(2, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_EndTime")) {
//								resultMap.put(3, Bytes.toString(kv.getValue()));
//							}
//							resultMap.put(4, userGroup.get(arr[2]));
//							if (Bytes.toString(kv.getQualifier()).equals("RTP")) {
//								resultMap.put(5, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals("其它")) {
//								resultMap.put(6, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"H.323")) {
//								resultMap.put(7, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals("SIP")) {
//								resultMap.put(8, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier())
//									.equals("MGCP")) {
//								resultMap.put(9, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GWNum")) {
//								System.out.println("^^^^^^map 10^^^^^^^^^");
//								resultMap
//										.put(10, Bytes.toString(kv.getValue()));
//								System.out.println("^^^^^^map 10:"
//										+ resultMap.get(10));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GWKeeperNum")) {
//								resultMap
//										.put(11, Bytes.toString(kv.getValue()));
//								System.out.println("^^^^^^map 11:"
//										+ resultMap.get(11));
//							}
//						}
//						for (int i = 1; i < resultMap.size(); i++) {
//							System.out.println("@@@" + i + " :"
//									+ resultMap.get(i));
//						}
//						if (index == 9) {
//							totalRows++;
//							for (int i = 1; i <= resultMap.size(); i++) {
//								sm_jsonArray.add(resultMap.get(i));
//							}
//							m_jsonArray.add(sm_jsonArray);
//							resultMap.clear();
//							sm_jsonArray.clear();
//							index = 0;
//						}
//						index++;
//					}
//				}
//			}
//			jsonObject.put("aaData", m_jsonArray);
//			jsonObject.put("iTotalDisplayRecords", totalRows);
//			jsonObject.put("iTotalRecords", totalRows);
//		}
//
//		return jsonObject;
//	}
//
//	public JSONObject voipGWLogHbase(String date1, String date2,
//			Map<String, String> userGroup, List<String> devName,
//			int iDisplayStart, int iDisplayLength) throws IOException {
//		JSONObject jsonObject = new JSONObject();
//		Filter filter = new PageFilter(iDisplayLength);
//		int totalRows = 0;
//		int total = 0;
//		byte[] lastRow = Bytes.toBytes(iDisplayStart);
//		System.out.println(lastRow.length);
//		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
//			JSONArray m_jsonArray = new JSONArray();
//			for (String dev : devName) {
//				String rowkey = dev + "\001" + date1 + "\001"
//						+ userEntry.getKey();
//				Get get = new Get(Bytes.toBytes(rowkey));
//				get.setMaxVersions();
//				get.setTimeRange(0L, Long.MAX_VALUE);
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GW_IPLength"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GW_IP"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("TotalCallSessions"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CallSessionsConcurrent"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("TotalCallDurations"));
//				get.setFilter(filter);
//				Result rs = table.get(get);
//				if (lastRow != null) {
//					byte[] StartRow = Bytes.add(lastRow, POSTFIX);
//					System.out.println("start row:" + Bytes.toInt(StartRow));
//					// scan.setStartRow(StartRow);
//				}
//				int index = 1;
//				Map<Integer, String> rMap = new HashMap<Integer, String>();
//				JSONArray sm_jsonArray = new JSONArray();
//				if (rs.list() != null) {
//					for (KeyValue kv : rs.list()) {
//						String[] arr = Bytes.toString(kv.getRow())
//								.split("\001");
//						if (index <= 7) {
//							rMap.put(1, arr[0]);
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_StartTime")) {
//								rMap.put(2, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_EndTime")) {
//								rMap.put(3, Bytes.toString(kv.getValue()));
//							}
//							rMap.put(4, userGroup.get(arr[2]));
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GW_IPLength")) {
//								rMap.put(5, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GW_IP")) {
//								rMap.put(6, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"TotalCallSessions")) {
//								rMap.put(7, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CallSessionsConcurrent")) {
//								rMap.put(8, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"TotalCallDurations")) {
//								rMap.put(9, Bytes.toString(kv.getValue()));
//							}
//						}
//						if (index == 7) {
//							total++;
//							for (int i = 1; i <= rMap.size(); i++) {
//								sm_jsonArray.add(rMap.get(i));
//							}
//							m_jsonArray.add(sm_jsonArray);
//							sm_jsonArray.clear();
//							rMap.clear();
//							index = 0;
//						}
//						index++;
//					}
//				}
//				jsonObject.put("aaData", m_jsonArray);
//				jsonObject.put("iTotalDisplayRecords", total);
//				jsonObject.put("iTotalRecords", total);
//			}
//		}
//		return jsonObject;
//	}
//
//	public JSONObject voipCalledLogHbase(String date1, String date2,
//			Map<String, String> userGroup, List<String> devName,
//			int iDisplayStart, int iDisplayLength) throws IOException {
//		JSONObject jsonObject = new JSONObject();
//		Filter filter = new PageFilter(iDisplayLength);
//		int totalRows = 0;
//		int total = 0;
//		byte[] lastRow = Bytes.toBytes(iDisplayStart);
//		System.out.println(lastRow.length);
//		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
//			JSONArray m_jsonArray = new JSONArray();
//
//			for (String dev : devName) {
//				Map<Integer, String> gwMap = new HashMap<Integer, String>();
//				JSONArray sm_jsonArray = new JSONArray();
//				String rowkey = dev + "\001" + date1 + "\001"
//						+ userEntry.getKey();
//				Get get = new Get(Bytes.toBytes(rowkey));
//				get.setMaxVersions();
//				get.setTimeRange(0L, Long.MAX_VALUE);
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("GWKeeper_IPLength"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWKeeper_IP"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CalledNumber_Length"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CalledNumber"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CalledDuration"));
//				get.setFilter(filter);
//				Result rs = table.get(get);
//				int index = 1;
//				if (rs.list() != null) {
//					for (KeyValue kv : rs.list()) {
//						String[] arr = Bytes.toString(kv.getRow())
//								.split("\001");
//						if (index <= 7) {
//							gwMap.put(1, arr[0]);
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_StartTime")) {
//								gwMap.put(2, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_EndTime")) {
//								gwMap.put(3, Bytes.toString(kv.getValue()));
//							}
//							gwMap.put(4, userGroup.get(arr[2]));
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GWKeeper_IPLength")) {
//								gwMap.put(5, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GWKeeper_IP")) {
//								gwMap.put(6, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CalledNumber_Length")) {
//								gwMap.put(7, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CalledNumber")) {
//								gwMap.put(8, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CalledDuration")) {
//								gwMap.put(9, Bytes.toString(kv.getValue()));
//							}
//						}
//						if (index == 7) {
//							total++;
//							for (Map.Entry<Integer, String> gwEntry : gwMap
//									.entrySet()) {
//								sm_jsonArray.add(gwEntry.getValue());
//							}
//							m_jsonArray.add(sm_jsonArray);
//							sm_jsonArray.clear();
//							gwMap.clear();
//							index = 0;
//						}
//						index++;
//					}
//				}
//			}
//			jsonObject.put("aaData", m_jsonArray);
//			jsonObject.put("iTotalDisplayRecords", total);
//			jsonObject.put("iTotalRecords", total);
//		}
//		return jsonObject;
//
//	}
//
//	public JSONObject voipCallerLogHbase(String date1, String date2,
//			Map<String, String> userGroup, List<String> devName,
//			int iDisplayStart, int iDisplayLength) throws IOException {
//		JSONObject jsonObject = new JSONObject();
//		Filter filter = new PageFilter(iDisplayLength);
//		int totalRows = 0;
//		int total = 0;
//		byte[] lastRow = Bytes.toBytes(iDisplayStart);
//		System.out.println(lastRow.length);
//		for (Map.Entry<String, String> userEntry : userGroup.entrySet()) {
//			JSONArray m_jsonArray = new JSONArray();
//			for (String dev : devName) {
//				Map<Integer, String> gwMap = new HashMap<Integer, String>();
//				JSONArray sm_jsonArray = new JSONArray();
//				String rowkey = date1 + "\001" + dev + "\001"
//						+ userEntry.getKey();
//				Get get = new Get(Bytes.toBytes(rowkey));
//				get.setMaxVersions();
//				get.setTimeRange(0L, Long.MAX_VALUE);
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_StartTime"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("R_EndTime"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("GWKeeper_IPLength"));
//				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("GWKeeper_IP"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CallerNumber_Length"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CallerNumber"));
//				get.addColumn(Bytes.toBytes("cf"),
//						Bytes.toBytes("CallerDuration"));
//				get.setFilter(filter);
//				Result rs = table.get(get);
//				int index = 1;
//				if (rs.list() != null) {
//					for (KeyValue kv : rs.list()) {
//						String[] arr = Bytes.toString(kv.getRow())
//								.split("\001");
//						if (index <= 7) {
//							gwMap.put(1, arr[0]);
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_StartTime")) {
//								gwMap.put(2, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"R_EndTime")) {
//								gwMap.put(3, Bytes.toString(kv.getValue()));
//							}
//							gwMap.put(4, userGroup.get(arr[2]));
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GWKeeper_IPLength")) {
//								gwMap.put(5, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"GWKeeper_IP")) {
//								gwMap.put(6, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CallerNumber_Length")) {
//								gwMap.put(7, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CallerNumber")) {
//								gwMap.put(8, Bytes.toString(kv.getValue()));
//							}
//							if (Bytes.toString(kv.getQualifier()).equals(
//									"CallerDuration")) {
//								gwMap.put(9, Bytes.toString(kv.getValue()));
//							}
//						}
//						if (index == 7) {
//							total++;
//							for (Map.Entry<Integer, String> gwEntry : gwMap
//									.entrySet()) {
//								sm_jsonArray.add(gwEntry.getValue());
//							}
//							m_jsonArray.add(sm_jsonArray);
//							sm_jsonArray.clear();
//							gwMap.clear();
//							index = 0;
//						}
//						index++;
//					}
//				}
//			}
//			jsonObject.put("aaData", m_jsonArray);
//			jsonObject.put("iTotalDisplayRecords", total);
//			jsonObject.put("iTotalRecords", total);
//		}
//		return jsonObject;
//	}

	/**
	 * 
	 * 
	 * The part of the DAY
	 */
	public JSONArray voipFlowTrdHbase_day(String date1, String date2,
			String userGroup, List<String> protName, List<String> devName)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		for (String prot : protName) {
			Result[] rst = null;
			Map<String, Double> protMap = new HashMap<String, Double>();
			for (int i = 0; i <= lDate.size() - 1; i++) {
				protMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
			}
			JSONArray sm_jsonArray = new JSONArray();
			for (String dev : devName) {
				List<Get> getList = new ArrayList<Get>();
				for (Date day : lDate) {
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ dev + "\001" + userGroup;
					System.out.println("------rowkey :" + rowkey);
					System.out.println("------prot :" + prot);
					Get get = new Get(Bytes.toBytes(rowkey));
					get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(prot));
					getList.add(get);
				}
				rst = tableDay.get(getList);

				for (int i = 0; i < rst.length - 1; i++) {
					for (KeyValue kv : rst[i].raw()) {
						String[] arr = Bytes.toString(kv.getRow())
								.split("\001");
						protMap.put(
								arr[0],
								protMap.get(arr[0])
										+ Double.parseDouble((Bytes.toString(kv
												.getValue()))));
						System.out.println("***this is the test output***");
						System.out.print(Bytes.toString(kv.getKey()) + "  ");
						System.out.print(arr[0] + "  ");
						System.out
								.println(Bytes.toString(kv.getValue()) + "  ");
						System.out.println("***this is the test output***");
					}
				}
			}
			JSONArray jsonarr = new JSONArray();
			jsonarr.add(prot);
			for (int i = 0; i < lDate.size() - 1; i++) {
				// System.out.print(entry.getValue() + ",");
				sm_jsonArray.add(protMap.get(sdf.format(lDate.get(i)).replace(
						'-', '/')));
			}
			jsonarr.add(sm_jsonArray);
			jsonArray.add(jsonarr);

		}

		return jsonArray;
	}

	public JSONObject voipFlowLogHbase_day(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);

		String[] column = { "R_StartTime", "R_EndTime", "RTP", "其它", "H.323",
				"SIP", "MGCP", "GWNum", "GWKeeperNum" };
		Result[] result = new Result[column.length];
		JSONObject allObject = new JSONObject();
		JSONArray outArray = new JSONArray();
		for (Map.Entry<String, String> user : userGroup.entrySet())
			for (String dev : devName) {
				for (Date day : lDate) {
					JSONArray propertyArray = new JSONArray();
					System.out.println("....." + devName);
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ dev + "\001" + user.getKey();
					System.out.println("rowkey" + rowkey);
					for (int no = 0; no <= column.length - 1; no++) {
						System.out.println("column " + column[no]);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.setMaxVersions();
						get.setTimeRange(0L, Long.MAX_VALUE);
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(column[no]));
						result[no] = table.get(get);
						System.out.println("resultlength" + result.length);
					}

					if (result[1] .getRow()!= null) {
						total = result[1].size();
						int realLength=iDisplayLength+iDisplayStart;
						if(realLength>=result[1].size()){
							realLength=result[1].size();
						}
						for (int kno = iDisplayStart; kno <=realLength-1; kno++) {
							Map<Integer, String> resultMap = new HashMap<Integer, String>();
							propertyArray.add(dev);
							total = result[1].size();
							localRows++;
							for (int i = 0; i <= result.length - 1; i++) {
								String[] rowkeyarr = Bytes.toString(
										result[i].getRow()).split("\001");
								if (i < 2) {
									resultMap.put(
											i,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								if (i == 2) {
									resultMap.put(2, rowkeyarr[2]);
									resultMap.put(
											3,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));

								}
								if (i > 2) {
									resultMap.put(
											i + 1,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								System.out.println("##resultMap:" + i + "-->"
										+ resultMap.get(i));
							}
							for (int mno = 0; mno <= resultMap.size() - 1; mno++) {
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

	public JSONObject voipGWLogHbase_day(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows =iDisplayStart;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		String[] column = { "R_StartTime", "R_EndTime", "GW_IPLength", "GW_IP", "TotalCallSessions",
				"CallSessionsConcurrent", "TotalCallDurations"};
		Result[] result = new Result[column.length];
		JSONObject allObject = new JSONObject();
		JSONArray outArray = new JSONArray();
		for (Map.Entry<String, String> user : userGroup.entrySet())
			for (String dev : devName) {
				for (Date day : lDate) {
					JSONArray propertyArray = new JSONArray();
					System.out.println("....." + devName);
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ dev + "\001" + user.getKey();
					System.out.println("rowkey" + rowkey);
					for (int no = 0; no <= column.length - 1; no++) {
						System.out.println("column " + column[no]);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.setMaxVersions();
						get.setTimeRange(0L, Long.MAX_VALUE);
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(column[no]));
						result[no] = table.get(get);
						System.out.println("resultlength" + result.length);
					}

					if (result[1].getRow() != null) {
						total = result[1].size();
						int realLength=iDisplayLength+iDisplayStart-1;
						if(realLength>=result[1].size()){
							realLength=result[1].size();
						}
						for (int kno = iDisplayStart-1; kno <=realLength-1; kno++) {
							Map<Integer, String> resultMap = new HashMap<Integer, String>();
							propertyArray.add(dev);
							localRows++;
							for (int i = 0; i <= result.length - 1; i++) {
								String[] rowkeyarr = Bytes.toString(
										result[i].getRow()).split("\001");
								if (i < 2) {
									resultMap.put(
											i,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								if (i == 2) {
									resultMap.put(2, rowkeyarr[2]);
									resultMap.put(
											3,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));

								}
								if (i > 2) {
									resultMap.put(
											i + 1,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								System.out.println("##resultMap:" + i + "-->"
										+ resultMap.get(i));
							}
							for (int mno = 0; mno <= resultMap.size() - 1; mno++) {
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
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords", total);
		return allObject;
	}

	public JSONObject voipCalledLogHbase_day(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		String[] column = { "R_StartTime", "R_EndTime", "GWKeeper_IPLength", "GWKeeper_IP", "CalledNumber_Length",
				"CalledNumber", "CalledDuration"};
		Result[] result = new Result[column.length];
		JSONObject allObject = new JSONObject();
		JSONArray outArray = new JSONArray();
		for (Map.Entry<String, String> user : userGroup.entrySet())
			for (String dev : devName) {
				for (Date day : lDate) {
					JSONArray propertyArray = new JSONArray();
					System.out.println("....." + devName);
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ dev + "\001" + user.getKey();
					System.out.println("rowkey" + rowkey);
					for (int no = 0; no <= column.length - 1; no++) {
						System.out.println("column " + column[no]);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.setMaxVersions();
						get.setTimeRange(0L, Long.MAX_VALUE);
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(column[no]));
						result[no] = table.get(get);
						System.out.println("resultlength" + result.length);
					}

					if (result[1].getRow() != null) {
						total = result[1].size();
						int realLength=iDisplayLength+iDisplayStart-1;
						if(realLength>=result[1].size()){
							realLength=result[1].size();
						}
						for (int kno = iDisplayStart-1; kno <=realLength-1; kno++) {
							Map<Integer, String> resultMap = new HashMap<Integer, String>();
							propertyArray.add(dev);
							localRows++;
							for (int i = 0; i <= result.length - 1; i++) {
								String[] rowkeyarr = Bytes.toString(
										result[i].getRow()).split("\001");
								if (i < 2) {
									resultMap.put(
											i,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								if (i == 2) {
									resultMap.put(2, rowkeyarr[2]);
									resultMap.put(
											3,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));

								}
								if (i > 2) {
									resultMap.put(
											i + 1,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								System.out.println("##resultMap:" + i + "-->"
										+ resultMap.get(i));
							}
							for (int mno = 0; mno <= resultMap.size() - 1; mno++) {
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
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords", total);
		return allObject;

	}

	public JSONObject voipCallerLogHbase_day(String date1, String date2,
			Map<String, String> userGroup, List<String> devName,
			int iDisplayStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONObject jsonObject = new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		String[] column = { "R_StartTime", "R_EndTime", "GWKeeper_IPLength", "CallerNumber_Length", "CallerNumber",
				"CallerNumber", "CallerDuration"};
		Result[] result = new Result[column.length];
		JSONObject allObject = new JSONObject();
		JSONArray outArray = new JSONArray();
		for (Map.Entry<String, String> user : userGroup.entrySet())
			for (String dev : devName) {
				for (Date day : lDate) {
					JSONArray propertyArray = new JSONArray();
					System.out.println("....." + devName);
					String rowkey = sdf.format(day).replace('-', '/') + "\001"
							+ dev + "\001" + user.getKey();
					System.out.println("rowkey" + rowkey);
					for (int no = 0; no <= column.length - 1; no++) {
						System.out.println("column " + column[no]);
						Get get = new Get(Bytes.toBytes(rowkey));
						get.setMaxVersions();
						get.setTimeRange(0L, Long.MAX_VALUE);
						get.addColumn(Bytes.toBytes("cf"),
								Bytes.toBytes(column[no]));
						result[no] = table.get(get);
						System.out.println("resultlength" + result.length);
					}

					if (result[1].getRow()!= null) {
						int realLength=iDisplayLength+iDisplayStart-1;
						if(realLength>=result[1].size()){
							realLength=result[1].size();
						}
						for (int kno = iDisplayStart-1; kno <=realLength-1; kno++) {
							total = result[1].size();
							Map<Integer, String> resultMap = new HashMap<Integer, String>();
							propertyArray.add(dev);
							localRows++;
							for (int i = 0; i <= result.length - 1; i++) {
								String[] rowkeyarr = Bytes.toString(
										result[i].getRow()).split("\001");
								if (i < 2) {
									resultMap.put(
											i,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								if (i == 2) {
									resultMap.put(2, rowkeyarr[2]);
									resultMap.put(
											3,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));

								}
								if (i > 2) {
									resultMap.put(
											i + 1,
											Bytes.toString(result[i].list()
													.get(kno).getValue()));
								}
								System.out.println("##resultMap:" + i + "-->"
										+ resultMap.get(i));
							}
							for (int mno = 0; mno <= resultMap.size() - 1; mno++) {
								System.out.println("*****resultMap:"
										+ mno + "-->"
										+ resultMap.get(mno));
								propertyArray.add(resultMap.get(mno));
							}
							
							outArray.add(propertyArray);
							propertyArray.clear();

						}
					}

				}
			}

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords", total);
		return allObject;

	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseVoipTable rbv = new ReadHBaseVoipTable("10.10.2.41", "60000",
				"10.10.2.41", "2181");
		// String[] userStr = args[0].split("\001");
		// String[] devStr = args[1].split("\001");
		Map<String, String> userMap = new HashMap<String, String>();
		// userMap.put(userStr[0], userStr[1]);
		userMap.put("0", "哈哈哈");
		List<String> devList = new ArrayList<String>();
		// devList.add(devStr[0]);
		// devList.add(devStr[1]);
		devList.add("测试");
		devList.add("pp");
		devList.add("angry");
		devList.add("ttt");
		List<String> protList = new ArrayList<String>();
		// protList.add("SIP");
		// protList.add("MGCP");
		protList.add("RTP");
		JSONArray jsonArray = new JSONArray();
		JSONObject jsonObject = new JSONObject();
		JSONObject jsonObject1 = new JSONObject();
		JSONObject jsonObject2 = new JSONObject();
		JSONObject jsonObject3 = new JSONObject();
		try {
			jsonArray = rbv.voipFlowTrdHbase_day("2014/11/18", "2014/11/19",
					"0", protList, devList);
			jsonObject1 = rbv.voipGWLogHbase_day("2014/11/18", "2014/11/19",
					userMap, devList, 1, 2);
			jsonObject = rbv.voipFlowLogHbase_day("2014/11/18", "2014/11/19",
					userMap, devList, 1, 2);
			jsonObject2 = rbv.voipCalledLogHbase_day("2014/11/18",
					"2014/11/19", userMap, devList, 1, 2);
			jsonObject3 = rbv.voipCallerLogHbase_day("2014/11/18",
					"2014/11/19", userMap, devList, 1, 2);
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
