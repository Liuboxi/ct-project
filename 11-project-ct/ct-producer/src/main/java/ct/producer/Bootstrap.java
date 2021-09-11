package ct.producer;

import ct.common.bean.Producer;
import ct.producer.bean.LocalFileProducer;
import ct.producer.io.LocalFileDataIn;
import ct.producer.io.LocalFileDataOut;

import java.io.IOException;

/**
 * 启动对象
 */
public class Bootstrap {
    public static void main(String[] args) throws IOException {
        if(args.length < 2){
            System.out.println("系统参数不正确，请按照指定格式传递参数:java -jar Producer.jar path1 path2");
            System.exit(1);
        }

        //构建生产者
        Producer producer = new LocalFileProducer();

        //producer.setIn(new LocalFileDataIn("E:\\3.Project\\3.大数据技术之电信客服综合案例\\2.资料\\辅助文档\\contact.log"));
        //producer.setOut(new LocalFileDataOut("E:\\3.Project\\3.大数据技术之电信客服综合案例\\2.资料\\辅助文档\\callLog.log"));
        producer.setIn(new LocalFileDataIn(args[0]));
        producer.setOut(new LocalFileDataOut(args[1]));

        //生产数据
        producer.produce();

        //关闭生产对象
        producer.close();

    }
}
