package com.panxt.gmall.util;

import java.beans.SimpleBeanInfo;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * @author panxt
 * @create 2019-06-26 18:20
 */
public class DateUtil {

    /**
     * 获取时间戳日期 : yyyy-MM-dd
     * @param ts
     * @return
     */
    public static String getDate(Long ts){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd");
        return sdf.format(new Date(ts));
    }

    /**
     * 获取时间戳日期小时 : HH
     * @param ts
     * @return
     */
    public static String getDateHour(Long ts){
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH");

        final String time = sdf.format(new Date(ts));
        final String hour = time.split(" ")[1];
        return hour;
    }
}
