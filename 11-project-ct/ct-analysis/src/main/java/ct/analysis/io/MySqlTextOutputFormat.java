package ct.analysis.io;

import ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * MySql数据格式的输入对象
 */
public class MySqlTextOutputFormat extends OutputFormat<Text,Text> {

    protected static class MySqlRecordWriter extends RecordWriter<Text, Text> {
        private Connection connection = null;
        Map<String, Integer> userMap = new HashMap<>();
        Map<String, Integer> dateMap = new HashMap<>();


        /**
         * 构造函数，获取数据库连接，并获取电话号码和日期对应的id
         */
        public MySqlRecordWriter() {
            //获取数据库连接
            connection = JDBCUtil.getConnection();
            PreparedStatement ps = null;
            ResultSet rs = null;

            try {

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

                    //封装到userMap中
                    userMap.put(tel, id);
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
                    if (day.length() == 1) {
                        day = "0" + day;
                    }
                    dateMap.put(year + month + day, id);
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

        /**
         * 输出数据
         *
         * @param key
         * @param value
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void write(Text key, Text value) throws IOException, InterruptedException {
            //获取reducer阶段输出数据
            String[] values = value.toString().split("_");

            //获取sumCall和sumDuration
            String sumCall = values[0];
            String sumDuration = values[1];

            //将分析后的数据插入到mysql中
            PreparedStatement pst = null;
            try {
                //插入数据
                String sql3 = "insert into ct_call(telid,dateid,sumcall,sumduration) values(?,?,?,?)";

                pst = connection.prepareStatement(sql3);

                //获取tel和date
                String[] keys = key.toString().split("_");
                String tel = keys[0];
                String date = keys[1];
                //向占位符中插入数据
                pst.setInt(1,userMap.get(tel));
                pst.setInt(2,dateMap.get(date));
                pst.setInt(3,Integer.parseInt(sumCall));
                pst.setInt(4,Integer.parseInt(sumDuration));


                //插入操作
                pst.executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                //关闭资源
                if(pst != null){
                    try {
                        pst.close();
                    } catch (SQLException throwables) {
                        throwables.printStackTrace();
                    }
                }

            }
        }


        /**
         * 关闭资源
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if(connection != null){
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
        }
    }

    @Override
    public RecordWriter<Text,Text> getRecordWriter(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        return new MySqlRecordWriter();
    }

    @Override
    public void checkOutputSpecs(JobContext jobContext) throws IOException, InterruptedException {

    }

    private FileOutputCommitter committer = null;

    public static Path getOutputPath(JobContext job) {
        String name = job.getConfiguration().get(FileOutputFormat.OUTDIR);
        return name == null ? null : new Path(name);
    }

    public OutputCommitter getOutputCommitter(TaskAttemptContext context) throws IOException, InterruptedException {
        if (committer == null) {
            Path output = getOutputPath(context);
            committer = new FileOutputCommitter(output, context);
        }
        return committer;
    }

}
