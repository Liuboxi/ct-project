package ct.common.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 日期工具类
 */
public class DateUtil {

    /**
     * 将日期对象转化为指定格式的日期字符串
     * @param date
     * @param format
     * @return
     */
    public static String format(Date date,String format){
        SimpleDateFormat sdf = new SimpleDateFormat(format);
        return sdf.format(date);
    }

    /**
     * 将日期字符串按照指定的格式解析为日期对象
     * @param dateString
     * @param format
     * @return
     */
    public static Date parse(String dateString, String format) {

        SimpleDateFormat sdf = new SimpleDateFormat(format);
        Date date = null;
        try {
            date = sdf.parse(dateString);
        } catch (ParseException e) {
            e.printStackTrace();
        }

        return date;
    }
}
