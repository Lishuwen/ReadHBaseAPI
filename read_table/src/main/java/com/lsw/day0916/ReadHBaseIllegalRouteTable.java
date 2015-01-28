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

public class ReadHBaseIllegalRouteTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table, tableday;
	private HBaseAdmin admin;

	public ReadHBaseIllegalRouteTable(String masterIP, String masterPort,
			String zkIp, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIp);

		try {
			table = new HTable(config, Bytes.toBytes("illegalroute_5minute"));
			tableday = new HTable(config, Bytes.toBytes("illegalroute_day"));
			admin = new HBaseAdmin(config);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject illegalRouteLogHbase(String date1, String date2,
			String nodeIP, String cpVal, List<String> devNameList,
			int iDisplayStart, int iDisplayLength) throws IOException {
		// JSONObject jsonObject=new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		String[] column = { "R_StartTime", "R_EndTime", "NodeInTraffic",
				"NodeOutTraffic", "CP_Length" };
		Result[] result = new Result[column.length];
		JSONObject allObject = new JSONObject();
		JSONArray outArray = new JSONArray();
		for (String devName : devNameList) {
			JSONArray propertyArray = new JSONArray();
			System.out.println("....." + devName);
			String rowkey = date1 + "\001" + devName+"\001" + nodeIP + "\001"
			+ cpVal;;
			System.out.println("rowkey" + rowkey);
			for (int no = 0; no <= column.length - 1; no++) {
				System.out.println("column " + column[no]);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.setMaxVersions();
				get.setTimeRange(0L, Long.MAX_VALUE);
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(column[no]));
				result[no] = table.get(get);
				System.out.println("resultlength" + result.length);
			}

			
			if (result[1].getRow() != null) {
				total=result[1].size();
				int realLength=iDisplayLength+iDisplayStart-1;
				if(realLength>=result[1].size()){
					realLength=result[1].size();
				}
				for (int kno = iDisplayStart-1; kno <=realLength-1; kno++) {
					
					Map<Integer, String> resultMap = new HashMap<Integer, String>();
					propertyArray.add(devName);
					localRows++;
					for (int i = 0; i <= result.length - 1; i++) {
						String[] rowkeyarr = Bytes.toString(result[i].getRow())
								.split("\001");
						if (i < 2) {
							resultMap.put(
									i,
									Bytes.toString(result[i].list().get(kno)
											.getValue()));
						}
						if (i == 2) {
							resultMap.put(2, rowkeyarr[2]);
							resultMap.put(
									3,
									Bytes.toString(result[i].list().get(kno)
											.getValue()));

						}
						if (i > 2) {
							resultMap.put(
									i + 1,
									Bytes.toString(result[i].list().get(kno)
											.getValue()));
							resultMap.put(6, rowkeyarr[3]);
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

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords", total);
		return allObject;

	}

	public JSONArray illRouteTrdHbase(String date, String nodeIP, String cpVal,
			String traffType, List<String> devid) throws IOException {
		JSONArray jsonArray = new JSONArray();
		Map<Integer, Double> traffMap = new HashMap<Integer, Double>();
		for (int i = 1; i < 289; i++) {
			traffMap.put(i, 0.0);
		}
		JSONArray sm_jsonArray = new JSONArray();

		for (String dev : devid) {
			String rowkey = date + "\001" + dev + "\001" + nodeIP + "\001"
					+ cpVal;
			System.out.println("------rowkey :" + rowkey);
			Get get = new Get(Bytes.toBytes(rowkey));
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(traffType));
			Result rs = table.get(get);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
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
				if (traffMap.get(timeStampNum) != null) {
					tmpSum = traffMap.get(timeStampNum);
				}
				traffMap.put(
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
		if (traffType.equals("NodeInTraffic")) {
			jsonarr.add("接入流量");
		} else if (traffType.equals("NodeOutTraffic")) {
			jsonarr.add("接出流量");
		}

		for (Map.Entry<Integer, Double> entry : traffMap.entrySet()) {
			// System.out.print(entry.getValue() + ",");
			sm_jsonArray.add(entry.getValue());
		}
		jsonarr.add(sm_jsonArray);
		jsonArray.add(jsonarr);
		return jsonArray;
	}

	/**
	 * 
	 * the part of the DAY
	 * 
	 * */
//	public JSONObject illegalRouteLogHbase_day(String date1, String date2,
//			String nodeIP, String cpVal, List<String> devNameList,
//			int iDisplayStart, int iDisplayLength) throws IOException {
//		// JSONObject jsonObject=new JSONObject();
//		DayList dy = new DayList();
//		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
//		List<Date> lDate = dy.findDates(date1, date2);
//		Filter filter = new PageFilter(iDisplayLength);
//		int localRows = iDisplayStart;
//		int total = 0;
//		byte[] lastRow = Bytes.toBytes(iDisplayStart);
//		System.out.println(lastRow.length);
//
//		String[] column = { "R_StartTime", "R_EndTime", "NodeInTraffic",
//				"NodeOutTraffic", "CP_Length" };
//		Result[] result = new Result[column.length];
//		JSONObject allObject = new JSONObject();
//		JSONArray outArray = new JSONArray();
//
//					for (String devName : devNameList) {
//						for (Date day : lDate) {
//							JSONArray propertyArray = new JSONArray();
//							System.out.println("....." + devName);
//							String rowkey = sdf.format(day).replace('-', '/')+ "\001" + devName+"\001" + nodeIP + "\001"
//							+ cpVal;;
//							System.out.println("rowkey" + rowkey);
//							for (int no = 0; no <= column.length - 1; no++) {
//								System.out.println("column " + column[no]);
//								Get get = new Get(Bytes.toBytes(rowkey));
//								get.setMaxVersions();
//								get.setTimeRange(0L, Long.MAX_VALUE);
//								get.addColumn(Bytes.toBytes("cf"),
//										Bytes.toBytes(column[no]));
//								result[no] = table.get(get);
//								System.out.println("resultlength"
//										+ result.length);
//							}
//
//							if (result[1] != null) {
//								for (int kno = iDisplayStart; kno <=iDisplayLength; kno++) {
//									Map<Integer, String> resultMap = new HashMap<Integer, String>();
//									propertyArray.add(devName);
//
//									for (int i = 0; i <= result.length - 1; i++) {
//										String[] rowkeyarr = Bytes.toString(result[i].getRow())
//												.split("\001");
//										if (i < 2) {
//											resultMap.put(
//													i,
//													Bytes.toString(result[i].list().get(kno)
//															.getValue()));
//										}
//										if (i == 2) {
//											resultMap.put(2, rowkeyarr[2]);
//											resultMap.put(
//													3,
//													Bytes.toString(result[i].list().get(kno)
//															.getValue()));
//
//										}
//										if (i > 2) {
//											resultMap.put(
//													i + 1,
//													Bytes.toString(result[i].list().get(kno)
//															.getValue()));
//										resultMap.put(6, rowkeyarr[3]);
//										}
//										
//										System.out.println("##resultMap:" + i + "-->"
//												+ resultMap.get(i));
//									}
//									for (int mno = 0; mno <= resultMap.size() - 1; mno++) {
//										System.out.println("*****resultMap:" + mno + "-->"
//												+ resultMap.get(mno));
//										propertyArray.add(resultMap.get(mno));
//									}
//
//									outArray.add(propertyArray);
//									propertyArray.clear();
//
//								}
//							}
//
//						}
//		}
//
//		allObject.put("aaData", outArray);
//		allObject.put("iTotalDisplayRecords", localRows);
//		allObject.put("iTotalRecords",total);
//		return allObject;
//	}

	public JSONArray illRouteTrdHbase_day(String date1, String date2,
			String nodeIP, String cpVal, String traffType, List<String> devid)
			throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray jsonArray = new JSONArray();
		Map<String, Double> traffMap = new HashMap<String, Double>();
		for (int i = 0; i <= lDate.size() - 1; i++) {
			traffMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
		}
		JSONArray sm_jsonArray = new JSONArray();
		for (String dev : devid) {
			List<Get> getList = new ArrayList<Get>();
			for (Date day : lDate) {
				String rowkey = sdf.format(day).replace('-', '/') + "\001"
						+ dev + "\001" + nodeIP + "\001" + cpVal;
				System.out.println("------rowkey :" + rowkey);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(traffType));
				getList.add(get);
			}
			Result[] rs = tableday.get(getList);
			for (int i = 0; i < rs.length - 1; i++) {
				for (KeyValue kv : rs[i].raw()) {
					String arr[] = Bytes.toString(kv.getRow()).split("\001");
					double tmpSum = 0.0;
					if (traffMap.get(arr[0]) != null) {
						tmpSum = traffMap.get(arr[0]);
					}
					traffMap.put(
							arr[0],
							tmpSum
									+ Double.parseDouble((Bytes.toString(kv
											.getValue()))));
					System.out.println("***this is the test output***");
					System.out.print(Bytes.toString(kv.getKey()) + "  ");
					System.out.print(arr[0] + "  ");
					System.out.println(Bytes.toString(kv.getValue()) + "  ");
					System.out.println("***this is the test output***");
				}
			}
		}
		JSONArray jsonarr = new JSONArray();
		if (traffType.equals("NodeInTraffic")) {
			jsonarr.add("接入流量");
		} else if (traffType.equals("NodeOutTraffic")) {
			jsonarr.add("接出流量");
		}

		for (int i = 0; i <= lDate.size() - 1; i++) {
			// System.out.print(entry.getValue() + ",");
			sm_jsonArray.add(traffMap.get(sdf.format(lDate.get(i)).replace('-',
					'/')));
		}
		jsonarr.add(sm_jsonArray);
		jsonArray.add(jsonarr);
		return jsonArray;
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		ReadHBaseIllegalRouteTable rirt = new ReadHBaseIllegalRouteTable(
				"10.10.2.41", "60000", "10.10.2.41", "2181");

		List<String> devList = new ArrayList<String>();
		devList.add("ttt");
		devList.add("aaa");
		devList.add("done");
		JSONObject jsonObject = new JSONObject();
		JSONArray jsonArray = new JSONArray();

		try {
			jsonObject = rirt.illegalRouteLogHbase("2014/11/19",
					"2014/11/19", "10.10.0.117", "APEC", devList,1, 2);
			jsonArray = rirt.illRouteTrdHbase_day("2014/11/19", "2014/11/20",
					"10.10.0.117", "APEC", "NodeInTraffic", devList);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		System.out.println("$$$$******this is the jsonobject*******");
		System.out.println(jsonObject.toString());
		System.out.println("$$$$******this is the jsonarray*******");
		System.out.println(jsonArray.toString());
	}

}
