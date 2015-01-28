package com.lsw.day0916;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import net.sf.json.JSONArray;
import net.sf.json.JSONObject;

import org.apache.commons.collections.CollectionUtils;
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

import com.lsw.other.AttackBean;
import com.lsw.other.DayList;

public class ReadHBaseAttackTable {

	/**
	 * @param args
	 */
	private Configuration config;
	private HTable table, tableDay;
	private HBaseAdmin admin;

	public ReadHBaseAttackTable(String masterIP, String masterPort,
			String zkIP, String zkPort) {
		config = HBaseConfiguration.create();
		config.set("hbase.master", masterIP + ":" + masterPort);
		config.set("hbase.zookeeper.property.clientPort", zkPort);
		config.set("hbase.zookeeper.quorum", zkIP);

		try {
			table = new HTable(config, Bytes.toBytes("ddosattack_5minute"));
			tableDay = new HTable(config, Bytes.toBytes("ddosattack_day"));
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

		JSONArray sm_jsonArray = new JSONArray();
		Map<Integer, Double> versionMap = new HashMap<Integer, Double>();
		for (int i = 1; i < 289; i++) {
			versionMap.put(i, 0.0);
		}
		String dev = null;
		for (String devname : devNameList) {
			// concat the rowkey with the input field
			String rowkey = date + "\001" + devname + "\001" + userGroupId
					+ "\001" + attackType;
			Get get = new Get(Bytes.toBytes(rowkey));
			get.getMaxVersions();
			get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(attackFiled));
			get.setTimeRange(0L, Long.MAX_VALUE);
			Result rst = table.get(get);
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
			for (KeyValue kv : rst.raw()) {
				String arr2[] = Bytes.toString(kv.getRow()).split("\001");
				dev = arr2[0];
				long time = kv.getTimestamp();
				String date2 = sdf.format(new Date(time));
				String arr[] = date2.substring(11, 19).split(":");
				int hour = Integer.parseInt(arr[0]);
				int min = Integer.parseInt(arr[1]);
				int sec = Integer.parseInt(arr[2]);
				int timeStampNum = (int) Math
						.ceil((hour * 3600 + min * 60 + sec) / (60 * 5));
				double sum = 0.0;
				if (versionMap.get(timeStampNum) != null) {
					sum = versionMap.get(timeStampNum);
				}
				versionMap.put(
						timeStampNum,
						sum
								+ Double.parseDouble((Bytes.toString(kv
										.getValue()))));
				System.out.println("date2:" + date2);
				System.out.println("***this is the test output***");
				System.out.print(Bytes.toString(kv.getKey()) + "  ");
				System.out.print(timeStampNum + "  ");
				System.out.println(Bytes.toString(kv.getValue()) + "  ");
				System.out.println("***this is the test output***");

			}

		}
		JSONArray jsonarr = new JSONArray();
		for (int i = 1; i < versionMap.size(); i++) {
			sm_jsonArray.add(versionMap.get(i));
		}
		// jsonarr.add(value);
		// jsonarr.add(userGroupId);
		jsonarr.add(sm_jsonArray);
		jsonArray.add(jsonarr);

		return jsonArray;
	}

	public JSONObject attackAreaHbase(String userGroupId, String date1,
			String date2, String attackType, ArrayList<String> devNameList,
			int iDisplayLengthStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		int localRows = iDisplayLengthStart;
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
		Set<String> AttackNameSet = new HashSet<String>();
		List<KeyValue> list = new ArrayList<KeyValue>();
		List<KeyValue> list2 = new ArrayList<KeyValue>();
		List<KeyValue> list3 = new ArrayList<KeyValue>();
		for (String devName : devNameList) {
			for (Date day : lDate) {
				// concat the rowkey with the input field
				String rowkey = sdf.format(day).replace('-', '/') + "\001"
						+ devName + "\001" + userGroupId + "\001" + attackType;
				System.out.println("#######*****%%%%" + rowkey);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.getMaxVersions();
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("AttackAreaName"));
				get.setTimeRange(0L, Long.MAX_VALUE);
				Get get2 = new Get(Bytes.toBytes(rowkey));
				get2.getMaxVersions();
				get2.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("AttackTraffic"));
				get2.setTimeRange(0L, Long.MAX_VALUE);
				Get get3 = new Get(Bytes.toBytes(rowkey));
				get3.getMaxVersions();
				get3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackNum"));
				get3.setTimeRange(0L, Long.MAX_VALUE);
				rst = table.get(get);
				rst2 = table.get(get2);
				rst3 = table.get(get3);
				list = rst.list();
				list2 = rst2.list();
				list3 = rst3.list();

				double num = 0.0;
				System.out.println("###this is the test output###");
				System.out.println("######this is the test output########");
				if (lastRow != null) {
					byte[] StartRow = Bytes.add(lastRow, POSTFIX);
					System.out.println("start row:" + Bytes.toInt(StartRow));
					// scan.setStartRow(StartRow);
				}
				System.out.println("result list is:#######");
				System.out.println("result list is:#######");
				if (list != null) {
					for (KeyValue attackName : list) {
						AttackNameSet
								.add(Bytes.toString(attackName.getValue()));
					}
					System.out.println("result list is:#######" + list.get(0));
					for (int i = 0; i <= list.size() - 1; i++) {

						String key = Bytes.toString(list.get(i).getValue());
						double NumValue = Double.parseDouble(Bytes
								.toString(list3.get(i).getValue()));
						double TrafficValue = Double.parseDouble(Bytes
								.toString(list2.get(i).getValue()));
						AttackBean ak = new AttackBean();
						if (attackMap.get(key) != null) {
							double tmpNum = attackMap.get(key).getAttackNum();
							ak.setAttackTraffic(NumValue + tmpNum);
							double tmpTraffic = attackMap.get(key)
									.getAttackTraffic();
							ak.setAttackNum((TrafficValue + tmpTraffic));
							attackMap.put(key, ak);
						} else {
							ak.setAttackTraffic(TrafficValue);
							ak.setAttackNum(NumValue);
							attackMap.put(key, ak);
						}
					}
				}
			}
		}
		// jsonArray.add(appType);
		total = AttackNameSet.size();
		List<String> NameList = new ArrayList<String>();
		CollectionUtils.addAll(NameList, AttackNameSet.iterator());

		int realLength = iDisplayLength + iDisplayLengthStart;
		if (realLength >= AttackNameSet.size()) {
			realLength = AttackNameSet.size();
		}
		for (int kno = iDisplayLengthStart; kno <= realLength - 1; kno++) {
			localRows++;
			sm_jsonArray.add("<a href=\"#\" onclick=\"load_AreaLog('"
					+ NameList.get(kno) + "')\">" + NameList.get(kno) + "</a>");
			sm_jsonArray.add(attackMap.get(NameList.get(kno)).getAttackNum());
			sm_jsonArray.add(attackMap.get(NameList.get(kno))
					.getAttackTraffic());
			jsonArray.add(sm_jsonArray);
			sm_jsonArray.clear();
		}
		jsonObject.put("aaData", jsonArray);
		jsonObject.put("iTotalDisplayRecords", localRows);
		jsonObject.put("iTotalRecords", total);
		return jsonObject;

	}

	public JSONObject attackAreaLogHbase(Map<String, String> userGroupId,
			String date1, String date2, Map<String, String> attackType,
			ArrayList<String> devNameList, String areaName,
			int iDisplayLengthStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		// Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayLengthStart;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayLengthStart);
		System.out.println(lastRow.length);
		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();
		String[] column = { "Attack_StartTime", "Attack_EndTime",
				"AttackAreaName", "AttackNum", "AttackTraffic", "AttackAreaNum" };
		Result[] result = new Result[column.length];
		for (Map.Entry<String, String> user : userGroupId.entrySet()) {
			for (Map.Entry<String, String> attack : attackType.entrySet()) {
				for (String dev : devNameList) {
					for (Date day : lDate) {
						JSONArray propertyArray = new JSONArray();
						System.out.println("....." + dev);
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + dev + "\001" + user.getKey()
								+ "\001" + attack.getKey();
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

							int realLength = iDisplayLength
									+ iDisplayLengthStart-1;
							if (realLength >= result[1].size()) {
								realLength = result[1].size();
							}
							for (int kno = iDisplayLengthStart-1; kno <= realLength - 1; kno++) {
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
										resultMap.put(3, rowkeyarr[3]);
										resultMap.put(
												4,
												Bytes.toString(result[i].list()
														.get(kno).getValue()));

									}
									if (i > 2) {
										resultMap.put(
												i + 2,
												Bytes.toString(result[i].list()
														.get(kno).getValue()));
									}
									System.out.println("##resultMap:" + i
											+ "-->" + resultMap.get(i));
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
			}
		}

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords", total);
		return allObject;
	}

	public static byte[] POSTFIX = new byte[] { 1 };

	public JSONObject attackLogDetailsHbase(Map<String, String> userGroupId,
			String date1, String date2, Map<String, String> appAttTypeMap,
			List<String> devNameList, int iDisplayStart, int iDisplayLength)
			throws IOException {
		// JSONObject jsonObject=new JSONObject();
		Filter filter = new PageFilter(iDisplayLength);
		int localRows = iDisplayStart;
		int total = 0;
		byte[] lastRow = Bytes.toBytes(iDisplayStart);
		System.out.println(lastRow.length);
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray outArray = new JSONArray();
		JSONObject allObject = new JSONObject();

		String[] column = { "Attack_StartTime", "Attack_EndTime",
				"AppAttackTraffic", "AppAttackRate", "AttackAreaNum" };
		Result[] result = new Result[column.length];
		for (Map.Entry<String, String> user : userGroupId.entrySet()) {
			for (Map.Entry<String, String> attack : appAttTypeMap.entrySet()) {
				for (String dev : devNameList) {
					for (Date day : lDate) {
						JSONArray propertyArray = new JSONArray();
						System.out.println("....." + dev);
						String rowkey = sdf.format(day).replace('-', '/')
								+ "\001" + dev + "\001" + user.getKey()
								+ "\001" + attack.getKey();
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
							int realLength = iDisplayLength + iDisplayStart-1;
							if (realLength >= result[1].size()) {
								realLength = result[1].size();
							}
							for (int kno = iDisplayStart-1; kno <= realLength - 1; kno++) {
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
										resultMap.put(3, rowkeyarr[3]);
										resultMap.put(
												4,
												Bytes.toString(result[i].list()
														.get(kno).getValue()));

									}
									if (i > 2) {
										resultMap.put(
												i + 2,
												Bytes.toString(result[i].list()
														.get(kno).getValue()));
									}
									System.out.println("##resultMap:" + i
											+ "-->" + resultMap.get(i));
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
			}
		}

		allObject.put("aaData", outArray);
		allObject.put("iTotalDisplayRecords", localRows-1);
		allObject.put("iTotalRecords", total);
		return allObject;
	}

	public JSONArray attackTotalHbase(String userGroup, String date1,
			String date2, String appAttType, ArrayList<String> devName)
			throws IOException {
		JSONArray jsonArray = new JSONArray();
		double AttackNum = 0.0, AttackTrafficGB = 0.0;
		JSONArray sjsonArray = new JSONArray();
		for (String dev : devName) {
			String rowkey = date1 + "\001" + dev + "\001" + userGroup + "\001"
					+ appAttType;
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
					AttackTrafficGB += Double.parseDouble(Bytes.toString(kv
							.getValue()));
				}
			}
		}

		AttackTrafficGB = Double.parseDouble(String.format("%.4f",
				AttackTrafficGB / 1024));
		sjsonArray.add(AttackNum);
		sjsonArray.add(1);
		sjsonArray.add(AttackTrafficGB);
		jsonArray.add(sjsonArray);
		return jsonArray;
	}

	/**
	 * 
	 * The part of DAY
	 * 
	 * */

	public JSONArray attackChart5MHbase_day(String userGroupId,
			String attackFiled, String attackType, String date1, String date2,
			ArrayList<String> devNameList) throws IOException {

		// for (Map.Entry<Integer, Double> entry : versionMap.entrySet()) {
		// versionMap.put(entry.getKey(), 0.0);
		// }
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray jsonArray = new JSONArray();
		JSONArray sm_jsonArray = new JSONArray();
		Map<String, Double> versionMap = new HashMap<String, Double>();
		for (int i = 0; i <= lDate.size() - 1; i++) {
			versionMap.put(sdf.format(lDate.get(i)).replace('-', '/'), 0.0);
		}
		Result[] rst = null;

		String dev = null;
		for (String devname : devNameList) {
			List<Get> getList = new ArrayList<Get>();
			for (Date day : lDate) {
				// concat the rowkey with the input field
				String rowkey = sdf.format(day).replace('-', '/') + "\001"
						+ devname + "\001" + userGroupId + "\001" + attackType;
				Get get = new Get(Bytes.toBytes(rowkey));

				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(attackFiled));
				getList.add(get);
			}
			rst = tableDay.get(getList);
			for (int i = 0; i < rst.length - 1; i++) {
				for (KeyValue kv : rst[i].raw()) {
					String arr2[] = Bytes.toString(kv.getRow()).split("\001");
					dev = arr2[1];
					versionMap.put(
							arr2[0],
							versionMap.get(arr2[0])
									+ Double.parseDouble((Bytes.toString(kv
											.getValue()))));
					System.out.println("***this is the test output***");
					System.out.print(Bytes.toString(kv.getKey()) + "  ");
					System.out.print(arr2[0] + "  ");
					System.out.println(Bytes.toString(kv.getValue()) + "  ");
					System.out.println("***this is the test output***");

				}
			}
		}
		JSONArray jsonarr = new JSONArray();
		for (int i = 0; i < lDate.size() - 1; i++) {
			sm_jsonArray.add(versionMap.get(sdf.format(lDate.get(i)).replace(
					'-', '/')));
		}
		// jsonarr.add(value);
		// jsonarr.add(userGroupId);
		jsonarr.add(sm_jsonArray);
		jsonArray.add(jsonarr);

		return jsonArray;

	}

	public JSONObject attackAreaHbase_day(String userGroupId, String date1,
			String date2, String attackType, ArrayList<String> devNameList,
			int iDisplayLengthStart, int iDisplayLength) throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		int localRows = iDisplayLengthStart;
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
		Set<String> AttackNameSet = new HashSet<String>();
		List<KeyValue> list = new ArrayList<KeyValue>();
		List<KeyValue> list2 = new ArrayList<KeyValue>();
		List<KeyValue> list3 = new ArrayList<KeyValue>();
		for (String devName : devNameList) {
			for (Date day : lDate) {
				// concat the rowkey with the input field
				String rowkey = sdf.format(day).replace('-', '/') + "\001"
						+ devName + "\001" + userGroupId + "\001" + attackType;
				System.out.println("#######*****%%%%" + rowkey);
				Get get = new Get(Bytes.toBytes(rowkey));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("AttackAreaName"));
				Get get2 = new Get(Bytes.toBytes(rowkey));
				get2.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("AttackTraffic"));
				Get get3 = new Get(Bytes.toBytes(rowkey));
				get3.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackNum"));
				rst = tableDay.get(get);
				rst2 = tableDay.get(get2);
				rst3 = tableDay.get(get3);
				list = rst.list();
				list2 = rst2.list();
				list3 = rst3.list();

				double num = 0.0;
				System.out.println("###this is the test output###");
				System.out.println("######this is the test output########");
				if (lastRow != null) {
					byte[] StartRow = Bytes.add(lastRow, POSTFIX);
					System.out.println("start row:" + Bytes.toInt(StartRow));
					// scan.setStartRow(StartRow);
				}
				System.out.println("result list is:#######");
				System.out.println("result list is:#######");
				if (list != null) {
					for (KeyValue attackName : list) {
						AttackNameSet
								.add(Bytes.toString(attackName.getValue()));
					}
					System.out.println("result list is:#######" + list.get(0));
					for (int i = 0; i <= list.size() - 1; i++) {

						String key = Bytes.toString(list.get(i).getValue());
						double NumValue = Double.parseDouble(Bytes
								.toString(list3.get(i).getValue()));
						double TrafficValue = Double.parseDouble(Bytes
								.toString(list2.get(i).getValue()));
						AttackBean ak = new AttackBean();
						if (attackMap.get(key) != null) {
							double tmpNum = attackMap.get(key).getAttackNum();
							ak.setAttackTraffic(NumValue + tmpNum);
							double tmpTraffic = attackMap.get(key)
									.getAttackTraffic();
							ak.setAttackNum((TrafficValue + tmpTraffic));
							attackMap.put(key, ak);
						} else {
							ak.setAttackTraffic(TrafficValue);
							ak.setAttackNum(NumValue);
							attackMap.put(key, ak);
						}
					}
				}
			}
		}
		// jsonArray.add(appType);
		total = AttackNameSet.size();
		List<String> NameList = new ArrayList<String>();
		CollectionUtils.addAll(NameList, AttackNameSet.iterator());

		int realLength = iDisplayLength + iDisplayLengthStart-1;
		if (realLength >= AttackNameSet.size()) {
			realLength = AttackNameSet.size();
		}
		for (int kno = iDisplayLengthStart-1; kno <= realLength - 1; kno++) {
			localRows++;
			sm_jsonArray.add("<a href=\"#\" onclick=\"load_AreaLog('"
					+ NameList.get(kno) + "')\">" + NameList.get(kno) + "</a>");
			sm_jsonArray.add(attackMap.get(NameList.get(kno)).getAttackNum());
			sm_jsonArray.add(attackMap.get(NameList.get(kno))
					.getAttackTraffic());
			jsonArray.add(sm_jsonArray);
			sm_jsonArray.clear();
		}

		jsonObject.put("aaData", jsonArray);
		jsonObject.put("iTotalDisplayRecords", localRows-1);
		jsonObject.put("iTotalRecords", total);
		return jsonObject;

	}

	public JSONArray attackTotalHbase_day(String userGroup, String date1,
			String date2, String appAttType, ArrayList<String> devName)
			throws IOException {
		DayList dy = new DayList();
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
		List<Date> lDate = dy.findDates(date1, date2);
		JSONArray jsonArray = new JSONArray();
		double AttackNum = 0.0, AttackTrafficGB = 0.0;
		JSONArray sjsonArray = new JSONArray();
		for (String dev : devName) {

			Result rs[] = null;
			List<Get> getList = new ArrayList<Get>();
			for (Date day : lDate) {
				String rowkey = sdf.format(day).replace('-', '/') + "\001"
						+ devName + "\001" + userGroup + "\001" + appAttType;
				Get get = new Get(Bytes.toBytes(rowkey));
				get.setMaxVersions();
				get.setTimeRange(0L, Long.MAX_VALUE);
				get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("AttackNum"));
				get.addColumn(Bytes.toBytes("cf"),
						Bytes.toBytes("AttackTraffic"));
				getList.add(get);
			}
			rs = tableDay.get(getList);
			for (int i = 0; i < rs.length - 1; i++) {
				for (KeyValue kv : rs[i].raw()) {
					if (Bytes.toString(kv.getQualifier()).equals("AttackNum")) {
						AttackNum += Double.parseDouble(Bytes.toString(kv
								.getValue()));
					}
					if (Bytes.toString(kv.getQualifier()).equals(
							"AttackTraffic")) {
						AttackTrafficGB += Double.parseDouble(Bytes.toString(kv
								.getValue()));
					}
				}
			}
		}

		AttackTrafficGB = Double.parseDouble(String.format("%.4f",
				AttackTrafficGB / 1024));
		sjsonArray.add(AttackNum);
		sjsonArray.add(1);
		sjsonArray.add(AttackTrafficGB);
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
		devList.add("g");
		devList.add("kk");
		devList.add("ttt");
		Map<String, String> appMap = new HashMap<String, String>();
		// appList.add("POCO");
		// / appList.add("lllll");
		appMap.put("1", "applicationType");
		Map<String, String> userMap = new HashMap<String, String>();

		userMap.put("0", "哈哈哈");
		try {
			jsonArray = rb.attackChart5MHbase_day("0", "AppAttackRate", "1",
					"2014/11/15", "2014/11/16", devList);
			jsonObject1 = rb.attackAreaHbase_day("0", "2014/11/15", "2014/11/16",
					"1", devList, 1, 10);
			jsonObject2 = rb.attackAreaLogHbase(userMap, "2014/11/15",
					"2014/11/16", appMap, devList, "power", 1, 10);
			jsonObject = rb.attackLogDetailsHbase(userMap, "2014/11/15",
					"2014/11/16", appMap, devList, 1, 10);
			jsonArray1 = rb.attackTotalHbase_day("0", "2014/11/15",
					"2014/11/6", "1", devList);

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
