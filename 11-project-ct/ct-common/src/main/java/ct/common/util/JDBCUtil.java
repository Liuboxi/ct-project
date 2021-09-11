package ct.common.util;

import java.io.IOException;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

public class JDBCUtil {

    public static Connection getConnection() {
        //1. 读取配置文件的基本信息
        InputStream is = JDBCUtil.class.getClassLoader().getResourceAsStream("jdbc.properties");

        Connection connection = null;
        try {
            Properties properties = new Properties();
            properties.load(is);

            String url = properties.getProperty("url");
            String user = properties.getProperty("user");
            String password = properties.getProperty("password");
            String driverName = properties.getProperty("driverName");

            //2. 加载驱动
            Class.forName(driverName);

            //3. 获取连接
            connection = DriverManager.getConnection(url, user, password);
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }

        return connection;
    }

}

