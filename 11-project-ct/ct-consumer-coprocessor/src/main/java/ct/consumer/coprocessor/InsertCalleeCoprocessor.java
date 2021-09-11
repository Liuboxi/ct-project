package ct.consumer.coprocessor;

import ct.common.bean.HBaseDao;
import ct.common.constant.Names;
import org.apache.hadoop.hbase.Coprocessor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 *
 * 使用协处理器保存被叫用户的数据
 *
 * 协处理器的使用
 * 1. 创建类
 * 2. 让表找到协处理类（和表有关联）
 * 3. 将项目打成jar包发布到hbase中（关联的jar包也需要发布），并且需要分发
 */
public class InsertCalleeCoprocessor extends BaseRegionObserver {

    // 方法的命名规则
    // login
    // logout
    // prePut
    // doPut ：模板方法设计模式
    //    存在父子类：
    //    父类搭建算法的骨架
    //    子类重写算法的细节
    // postPut


    /**
     * 保存主叫用户数据之后，由HBase自动保存被叫用户数据
     * @param e
     * @param put
     * @param edit
     * @param durability
     * @throws IOException
     */
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf(Names.TABLE.getValue()));

        //主叫用户rowKey
        String rowKey = Bytes.toString(put.getRow());
        String[] values = rowKey.split("_");

        CoprocessorDao dao = new CoprocessorDao();
        String call1 = values[1];
        String call2 = values[3];
        String callTime = values[2];
        String duration = values[4];
        String flag = values[5];

        //只有主叫用户保存数据后才需要触发被叫用户数据的存储
        if("1".equals(flag)){

            //获取rowKey
            String calleeRowKey = dao.getRegionNum(call2,callTime) + "_" + call2 + "_" + callTime + "_" + call1 + "_" + duration + "_0";
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
            table.put(calleePut);

            //关闭表
            table.close();
        }

    }

    private class CoprocessorDao extends HBaseDao{
        public int getRegionNum(String tel,String time){
            return genRegionNum(tel,time);
        }
    }
}
