package ct.producer.bean;

import ct.common.bean.DataIn;
import ct.common.bean.DataOut;
import ct.common.bean.Producer;
import ct.common.util.DateUtil;
import ct.common.util.NumberUtil;

import java.io.IOException;
import java.util.Date;
import java.util.List;
import java.util.Random;

public class LocalFileProducer implements Producer {
    private DataIn in;
    private DataOut out;
    private volatile boolean flag = true;

    @Override
    public void setIn(DataIn in) {
        this.in = in;
    }

    @Override
    public void setOut(DataOut out) {
        this.out = out;
    }

    /**
     * 生产数据
     */
    @Override
    public void produce() {
        //读取通讯录数据
        try {
            List<Contact> contacts = in.read(Contact.class);

            while (flag) {
                //从通讯录中随机查找两个电话号码（主叫，被叫）
                int call1Index = new Random().nextInt(contacts.size());
                int call2Index;
                while (true) {
                    call2Index = new Random().nextInt(contacts.size());
                    if (call1Index != call2Index) {
                        break;
                    }
                }
                Contact call1 = contacts.get(call1Index);
                Contact call2 = contacts.get(call2Index);

                //生成随机通话时间

                //1.设置时间范围
                String startDate = "20200101000000";
                String endDate = "20210101000000";

                long startTime = DateUtil.parse(startDate, "yyyyMMddHHmmss").getTime();
                long endTime = DateUtil.parse(endDate, "yyyyMMddHHmmss").getTime();

                //2.通话时间
                long callTime =  startTime + (long) ((endTime - startTime) * Math.random());
                //3.通话时间字符串
                String callTimeStr = DateUtil.format(new Date(callTime), "yyyyMMddHHmmss");

                //4.生成随机的通话时长
                String duration = NumberUtil.format(new Random().nextInt(3600), 4);

                //5.生成通话记录
                CallLog log = new CallLog(call1.getTel(), call2.getTel(), callTimeStr, duration);

                System.out.println(log);
                //6.将通话记录刷写到数据文件中
                out.write(log);

                //7.没秒中产生两个通话记录
                Thread.sleep(500);
            }

        } catch (IOException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void close() throws IOException {
        if (in != null) {
            in.close();
        }
        if (out != null) {
            out.close();
        }
    }
}
