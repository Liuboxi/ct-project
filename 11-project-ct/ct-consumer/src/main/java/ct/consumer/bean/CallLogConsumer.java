package ct.consumer.bean;

import ct.common.bean.Consumer;
import ct.consumer.dao.HBaseDao;
import ct.common.constant.Names;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;

/**
 * 通话日志消费对象
 */
public class CallLogConsumer implements Consumer {
    @Override
    public void consume() {
        try {
            //创建配置对象
            Properties pro = new Properties();
            pro.load(Thread.currentThread().getContextClassLoader().getResourceAsStream("consumer.properties"));

            //获取flume采集的数据
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(pro);

            //关注主题
            consumer.subscribe(Arrays.asList(Names.TOPIC.getValue()));

            //HBase数据访问对象
            HBaseDao dao = new HBaseDao();
            //初始化
            dao.init();

            //消费数据
            while (true){
                ConsumerRecords<String, String> consumerRecords = consumer.poll(100);
                for (ConsumerRecord<String, String> consumerRecord : consumerRecords) {
                    System.out.println(consumerRecord.value());

                    //利用插入字符串的方式插入数据
                    dao.insertData(consumerRecord.value());

                    //利用封装对象的方式插入数据
                    //CallLog callLog = new CallLog(consumerRecord.value());
                    //dao.insertData(callLog);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    /**
     * 关闭资源
     * @throws IOException
     */
    @Override
    public void close() throws IOException {

    }
}
