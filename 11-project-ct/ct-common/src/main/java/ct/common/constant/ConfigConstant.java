package ct.common.constant;

import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.ResourceBundle;

public class ConfigConstant {
    private static Map<String,String> map = new HashMap<>();

    static {
        //国际化
        ResourceBundle ct = ResourceBundle.getBundle("ct");
        Enumeration<String> enumerations = ct.getKeys();
        while (enumerations.hasMoreElements()){
            String key = enumerations.nextElement();
            String value = ct.getString(key);
            map.put(key,value);
        }

    }
    public static String getVal(String key){
        return map.get(key);
    }

    public static void main(String[] args) {
        System.out.println(ConfigConstant.getVal("cf.info"));
    }
}
