package ct.analysis.io;

import ct.common.util.JDBCUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;


public class MySqlRedisTextOutputFormat extends OutputFormat<Text,Text> {

    protected static class MySqlRecordWriter extends RecordWriter<Text, Text> {
        private Connection connection = null;
        private Jedis jedis = null;

        public MySqlRecordWriter(){
            //获取数据库连接
            try {
                connection = JDBCUtil.getConnection();
            } catch (Exception e) {
                e.printStackTrace();
            }
            jedis = new Jedis("192.168.103.7", 6379);
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
            PreparedStatement ps = null;
            try {
                //插入数据
                String sql3 = "insert into ct_call(telid,dateid,sumcall,sumduration) values(?,?,?,?)";

                ps = connection.prepareStatement(sql3);

                //获取tel和date
                String[] keys = key.toString().split("_");
                String tel = keys[0];
                String date = keys[1];

                //向占位符中插入数据
                ps.setInt(1, Integer.parseInt(jedis.hget("ct_user",tel)));
                ps.setInt(2, Integer.parseInt(jedis.hget("ct_date",date)));
                ps.setInt(3, Integer.parseInt(sumCall));
                ps.setInt(4, Integer.parseInt(sumDuration));

                ps.executeUpdate();

            } catch (SQLException e) {
                e.printStackTrace();
            } finally {
                //关闭资源
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
         * 关闭资源
         *
         * @param taskAttemptContext
         * @throws IOException
         * @throws InterruptedException
         */
        @Override
        public void close(TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException throwables) {
                    throwables.printStackTrace();
                }
            }
/*
            if(jedis != null){
                jedis.close();
            }
*/
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
