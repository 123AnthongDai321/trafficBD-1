package com.hitbd.proj.util;

import java.util.ArrayList;
import java.util.Date;

import com.hitbd.proj.Settings;
import sun.reflect.annotation.ExceptionProxy;

public class Serialization {
	/**
	 * 测试序列转换函数
	 */
	public static void main(String[] args) {
		String[] lis = { "3423", "8743", "8723", "9279" };
		Date[] lli = { new Date(112, 0, 1), new Date(112, 1, 1),
				new Date(253, 0, 1), new Date(283, 0, 1) };
		countExpireList(lis, lli);
	}

	/**
	 * 布尔类型转int
	 * @param bool
	 * @return 转换后的int值
	 */
	public static int booToInt(boolean bool) {
		if (bool == true)
			return 1;
		else
			return 0;
	}

	/**
	 * list序列转字符串
	 * @param list 要转换的list
	 * @return 转换后的字符串
	 */
	public static <E> String listToStr(ArrayList<E> list) {
		int i;
		String str = new String();
		if (list.size() == 0)
			return "";
		for (i = 0; i < list.size() - 1; i++) {
			str = str + list.get(i).toString();
			str = str + ",";
		}
		str = str + list.get(i);
		return str;
	}

	/**
	 * 字符串转list序列
	 * 
	 * @param str 要转换的字符串
	 * @return 转换后的list
	 */
	public static ArrayList<Integer> strToList(String str) {
		ArrayList<Integer> list = new ArrayList<Integer>();
		if (str.equals("") || str == null)
			return list;
		String[] s = str.split(",");
		for (int i = 0; i < s.length; i++) {
			list.add(Integer.valueOf(s[i]).intValue());
		}
		return list;
	}

	public static ArrayList<Long> longToList(String str) {
		ArrayList<Long> list = new ArrayList<Long>();
		if (str.equals("") || str == null)
			return list;
		String[] s = str.split(",");
		for (int i = 0; i < s.length; i++) {
			list.add(Long.valueOf(s[i]).longValue());
		}
		return list;
	}

	/**
	 * 去掉user_B中父亲列表的孩子
	 * 
	 * @param children_ids child
	 */
	public static ArrayList<Integer> deleteChild(ArrayList<Integer> children_ids, int child) {
		children_ids.remove(Integer.valueOf(child));
		return children_ids;
	}

	/**
	 * 判断两个list是否还有公共值
	 * 
	 * @param list1
	 * @param list2
	 */
	public static boolean isContain(ArrayList<Long> list1, ArrayList<Long> list2) {
		for (int i = 0; i < list1.size(); i++) {
			for (int j = 0; j < list2.size(); j++) {
				if (list1.get(i).equals(list2.get(j)))
					return true;
			}
		}
		return false;
	}

	/**
	 * 求过期时间列表
	 * @param parentIds
     * @param expireDates parentIds 对应过期时间 expireDates
	 */
	public static String countExpireList(String[] parentIds, Date[] expireDates) {
		String expirelist = new String();
		int chick = 0;
		for (int i = 0; i < parentIds.length; i++) {
			long temp = (expireDates[i].getTime() - Settings.BASETIME) / (1000 * 60 * 60 * 24);
			if (temp < 0)
				return null;
			if (expireDates[i].compareTo(Settings.MAXTIME) > 0) {
				temp = 0;
			} else
				temp = temp - chick;
			expirelist = expirelist + parentIds[i] + "," + String.valueOf(temp) + ",";
			chick = (int) (chick + temp);
		}
		return expirelist.substring(0, expirelist.length() - 1);
	}

}
