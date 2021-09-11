package ct.consumer.dao;

import ct.common.constant.Names;
import ct.common.constant.ValueConstant;
import ct.consumer.bean.CallLog;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;

public class HBaseDao extends ct.common.bean.HBaseDao {

    /**
     * 初始化操作
     * @throws IOException
     */
    public void init() throws IOException {
        start();

        createNamespaceNX(Names.NAMESPNAMES.getValue());

        createTableXX(Names.TABLE.getValue(),"ct.consumer.coprocessor.InsertCalleeCoprocessor",
                ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue(),Names.CF_CALLEE.getValue());

        //createTableXX(Names.TABLE.getValue(),null,ValueConstant.REGION_COUNT,Names.CF_CALLER.getValue());

        end();
    }

    /**
     * 向HBase中插入字符串数据
     * @param data
     */
    public void insertData(String data) throws IOException {
        //获取通话日志数据
        String[] values = data.split("\t");
        String call1 = values[0];
        String call2 = values[1];
        String callTime = values[2];
        String duration = values[3];

        //创建数据对象
        /**
         * rowkey设计
         *    1）长度原则
         *       最大值64KB，推荐长度为10 ~ 100byte
         *       最好8的倍数，能短则短，rowkey如果太长会影响性能
         *    2）唯一原则 ： rowkey应该具备唯一性
         *    3）散列原则
         *       3-1）盐值散列：不能使用时间戳直接作为rowkey
         *            在时间戳的rowkey前增加随机数
         *       3-2）字符串反转 ：常用于时间，电话  new StringBuilder.reverse()
         *       3-3) 计算分区号：类似于ashMap
         */
        String rowKey = genRegionNum(call1,callTime) + "_" + call1 + "_" + callTime + "_" + call2 + "_" + duration + "_1";

        //主叫用户
        Put put = new Put(Bytes.toBytes(rowKey));

        //获取列族
        byte[] family = Bytes.toBytes(Names.CF_CALLER.getValue());

        //添加列数据
        put.addColumn(family,Bytes.toBytes("call1"),Bytes.toBytes(call1));
        put.addColumn(family,Bytes.toBytes("call2"),Bytes.toBytes(call2));
        put.addColumn(family,Bytes.toBytes("callTime"),Bytes.toBytes(callTime));
        put.addColumn(family,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        put.addColumn(family,Bytes.toBytes("flag"),Bytes.toBytes("1"));

/*
        String calleeRowKey = genRegionNum(call2,callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";
        //被叫用户
        Put calleePut = new Put(Bytes.toBytes(calleeRowKey));
        //获取列族
        byte[] calleeFamily = Bytes.toBytes(Names.CF_CALLEE.getValue());
        //添加列数据
        calleePut.addColumn(calleeFamily,Bytes.toBytes("call1"),Bytes.toBytes(call2));
        calleePut.addColumn(calleeFamily,Bytes.toBytes("call2"),Bytes.toBytes(call1));
        calleePut.addColumn(calleeFamily,Bytes.toBytes("callTime"),Bytes.toBytes(callTime));
        calleePut.addColumn(calleeFamily,Bytes.toBytes("duration"),Bytes.toBytes(duration));
        calleePut.addColumn(calleeFamily,Bytes.toBytes("flag"),Bytes.toBytes("0"));
*/

        //保存数据
        ArrayList<Put> puts = new ArrayList<>();
        puts.add(put);
        //puts.add(calleePut);

        putData(Names.TABLE.getValue(),puts);

    }

    /**
     * 向HBase中插入封装成对象的数据
     * @param callLog
     */
    public void insertData(CallLog callLog) throws IOException, IllegalAccessException {
        callLog.setRowKey(genRegionNum(callLog.getCall1(),callLog.getCallTime()) + "_"
                + callLog.getCall1() + "_" + callLog.getCall2() + "_" + callLog.getDuration());
        putData(callLog);
    }
}
