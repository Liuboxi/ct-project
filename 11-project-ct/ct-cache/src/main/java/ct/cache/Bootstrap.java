package ct.cache;

import ct.common.util.JDBCUtil;
import redis.clients.jedis.Jedis;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 启动缓存端，向redis中缓存数据
 */
public class Bootstrap {
    public static void main(String[] args) {

        PreparedStatement ps = null;
        ResultSet rs = null;

        try {
            //获取数据库连接
            Connection connection = JDBCUtil.getConnection();
            Jedis jedis = new Jedis("192.168.103.7", 6379);

            //查询语句sql(获取电话号码和电话号码对应的id)
            String sql1 = "select id,tel from ct_user";
            //获取PreparedStatement
            ps = connection.prepareStatement(sql1);
            //获取查询结果集
            rs = ps.executeQuery();
            while (rs.next()) {
                //获取id值
                Integer id = rs.getInt(1);

                //获取电话号码
                String tel = rs.getString(2);

                //将获取到的数据存储到redis中
                jedis.hset("ct_user",tel,id+"");
            }

            //(获取日期信息和日期对应的id)
            String sql2 = "select id,year,month,day from ct_date";
            ps = connection.prepareStatement(sql2);
            rs = ps.executeQuery();
            while (rs.next()) {
                Integer id = rs.getInt(1);
                String year = rs.getString(2);
                String month = rs.getString(3);
                if (month.length() == 1) {
                    month = "0" + month;
                }
                String day = rs.getString(4);
                if (day.length() == 0) {
                    day = "0" + day;
                }
                jedis.hset("ct_date",year+month+day,id+"");
            }

        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            //关闭资源
            if (rs != null) {
                try {
                    rs.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
            if (ps != null) {
                try {
                    ps.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }

    }
}
