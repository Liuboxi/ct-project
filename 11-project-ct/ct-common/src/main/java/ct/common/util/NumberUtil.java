package ct.common.util;

import java.text.DecimalFormat;

/**
 * 数组工具类
 */
public class NumberUtil {

    /**
     * 将num转化为length长度的字符串
     * @param num
     * @param length
     * @return
     */
    public static String format(int num,int length){

        StringBuilder stringBuilder = new StringBuilder();
        for(int i = 1; i <= length; i++){
            stringBuilder.append("0");
        }

        DecimalFormat decimalFormat = new DecimalFormat(stringBuilder.toString());
        return decimalFormat.format(num);

    }
}
