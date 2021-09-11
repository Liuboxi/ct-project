package ct.common.bean;

import ct.common.annotation.Column;
import ct.common.annotation.RowKey;
import ct.common.annotation.TableRef;
import ct.common.constant.Names;
import ct.common.constant.ValueConstant;
import ct.common.util.DateUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.List;

/**
 * 基础数据访问对象
 */
public abstract class HBaseDao {
    //本地线程，确保当前线程中只有一个指定对象实例
    private ThreadLocal<Connection> connHolder = new ThreadLocal<>();
    private ThreadLocal<Admin> adminHolder = new ThreadLocal<>();

    //protected:确保只有子类中才能访问该方法
    protected void start() throws IOException {
        getConnection();
        getAdmin();
    }

    protected void end() throws IOException {
        Admin admin = getAdmin();
        if (admin != null) {
            admin.close();
            adminHolder.remove();
        }
        Connection connection = getConnection();
        if (connection != null) {
            connection.close();
            connHolder.remove();
        }
    }

    /**
     * 获取HBase连接对象
     *
     * @return
     */
    protected synchronized Connection getConnection() throws IOException {
        Connection connection = connHolder.get();
        if (connection == null) {
            //获取HBase配置信息
            Configuration configuration = HBaseConfiguration.create();
            //获取连接对象
            connection = ConnectionFactory.createConnection(configuration);
            connHolder.set(connection);
        }
        return connection;
    }

    /**
     * 获取HBase管理对象
     *
     * @return
     */
    protected synchronized Admin getAdmin() throws IOException {
        Admin admin = adminHolder.get();
        if (admin == null) {
            //获取管理对象
            admin = getConnection().getAdmin();
            adminHolder.set(admin);
        }
        return admin;
    }

    /**
     * 创建命名空间，如果命名空间也存在，不需要创建，否则，创建新的
     *
     * @param namespace
     */
    protected void createNamespaceNX(String namespace) throws IOException {
        Admin admin = getAdmin();

        try {
            //获取命名空间描述器,判断命名空间是否存在
            admin.getNamespaceDescriptor(namespace);

        } catch (NamespaceNotFoundException e) {
            //创建命名空间描述器
            NamespaceDescriptor namespaceDescriptor = NamespaceDescriptor.create(namespace).build();

            //创建命名空间
            admin.createNamespace(namespaceDescriptor);
        }
    }


    /**
     * 创建表
     *
     * @param name
     * @param families
     * @throws IOException
     */
    protected void createTableXX(String name, String... families) throws IOException {
        createTableXX(name, null, null,families);
    }

    /**
     * 创建表，如果表已存在，则删除后创建新的
     *
     * @param name
     * @param families
     */
    protected void createTableXX(String name, String coprocessorClass,Integer regionCount, String... families) throws IOException {
        Admin admin = getAdmin();

        TableName tableName = TableName.valueOf(name);

        if (admin.tableExists(tableName)) {
            //表存在，删除
            deleteTable(name);
        }

        //创建表
        createTable(name,coprocessorClass, regionCount, families);
    }


    /**
     * 创建表
     *
     * @param name
     * @param coprocessorClass
     * @param regionCount
     * @param families
     * @throws IOException
     */
    protected void createTable(String name, String coprocessorClass, Integer regionCount, String... families) throws IOException {
        //获取表名
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);

        //获取表描述器
        HTableDescriptor hTableDescriptor = new HTableDescriptor(tableName);

        //判断列族是否为空
        if (families == null || families.length == 0) {
            families = new String[1];
            families[0] = Names.CF_INFO.getValue();
        }

        //设置列族信息
        for (String family : families) {
            //获取列族描述器
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor(family);
            //添加列族信息
            hTableDescriptor.addFamily(hColumnDescriptor);

        }

        if (coprocessorClass != null && !"".equals(coprocessorClass)) {
            hTableDescriptor.addCoprocessor(coprocessorClass);
        }

        //增加预分区
        if (regionCount == null || regionCount <= 1) {
            //不需要分区，直接创建表
            admin.createTable(hTableDescriptor);
        } else {
            //分区键
            byte[][] splitKeys = genSplitKeys(regionCount);
            //创建分区表
            admin.createTable(hTableDescriptor, splitKeys);
        }


    }

    /**
     * 生成分区键
     *
     * @param regionCount
     * @return
     */
    private byte[][] genSplitKeys(int regionCount) {
        //分区键等于分区数减1
        int splitKeyCount = regionCount - 1;

        //分区键以字节数组的形式存储，而存储分区键字节数组的数据类型又为字节数组，因此产生二维数组
        byte[][] bs = new byte[splitKeyCount][];

        //设置分区键     eg: (-∞, 0|), [0|,1|), [1| +∞)
        List<byte[]> bsList = new ArrayList<>();
        for (int i = 0; i < splitKeyCount; i++) {
            String splitKey = i + "|";
            bsList.add(Bytes.toBytes(splitKey));
        }

        //将分区键排序
        Collections.sort(bsList, new Bytes.ByteArrayComparator());

        //将List转化为字节数组
        bsList.toArray(bs);

        return bs;
    }

    /**
     * 计算分区号：将同一用户（电话号码相同）同一年同一个月的数据放到同一个分区中
     *
     * @param tel
     * @param date
     * @return
     */
    protected int genRegionNum(String tel, String date) {
        //获取电话号码的后四位
        String userCode = tel.substring(tel.length() - 4);

        //获取日期的前六位（即年后和月信息）
        String yearAndMonth = date.substring(0, 6);

        //取哈希值
        int userCodeHash = userCode.hashCode();
        int yearAndeMonthHash = yearAndMonth.hashCode();

        //crc校验，采用异或算法,可能为负数，因此取绝对值
        int crc = Math.abs(userCodeHash ^ yearAndeMonthHash);

        //取模,得到分区号
        int regionNum = crc % ValueConstant.REGION_COUNT;

        return regionNum;
    }

    /**
     * 获取查询时startRow，stopRow集合
     *
     * @param tel
     * @param start
     * @param end
     * @return
     */
    protected List<String[]> getStartStopRowKeys(String tel, String start, String end) {
        //用于存储分区数据
        ArrayList<String[]> rowKeyss = new ArrayList<>();

        //获取首尾时间的年月信息
        String startTime = start.substring(0, 6);
        String endTime = end.substring(0, 6);

        //将首尾时间的年月信息转化为日历时间
        Calendar startCal = Calendar.getInstance();
        startCal.setTime(DateUtil.parse(startTime, "yyyyMM"));

        Calendar endCal = Calendar.getInstance();
        endCal.setTime(DateUtil.parse(endTime, "yyyyMM"));

        //循环遍历，获取指定首尾时间范围内每个月的通话日志
        while (startCal.getTimeInMillis() <= endCal.getTimeInMillis()) {
            //当前时间（将日历时间转换为字符串的时间格式）
            String nowTime = DateUtil.format(startCal.getTime(), "yyyyMM");

            //生成分区号
            int regionNum = genRegionNum(tel, nowTime);

            //获取rowKey
            String startRow = regionNum + "_" + tel + "_" + nowTime;
            String stopRow = startRow + "|";

            //将获取到的rowKey保存在String[]中   形式：{1_4989_202010,1_4989_202010|}
            String[] rowKeys = {startRow, stopRow};
            rowKeyss.add(rowKeys);

            //月份加1
            startCal.add(Calendar.MONTH, 1);
        }
        return rowKeyss;
    }


    /**
     * 删除表格
     *
     * @param name
     * @throws IOException
     */
    protected void deleteTable(String name) throws IOException {
        Admin admin = getAdmin();
        TableName tableName = TableName.valueOf(name);
        admin.disableTable(tableName);
        admin.deleteTable(tableName);
    }

    /**
     * 普通方式增加数据
     *
     * @param name
     * @param put
     * @throws IOException
     */
    protected void putData(String name, Put put) throws IOException {
        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));

        //增加数据
        table.put(put);

        //关闭表
        table.close();
    }

    /**
     * 通过反射的方式增加数据
     *
     * @param obj
     */
    protected void putData(Object obj) throws IOException, IllegalAccessException {
        //获取表名
        Class<?> clazz = obj.getClass();
        TableRef tableRef = clazz.getAnnotation(TableRef.class);
        String tableName = tableRef.value();

        //获取rowKey信息
        //1.获取所有属性的信息
        Field[] fs = clazz.getDeclaredFields();
        String rowKey = "";
        for (Field f : fs) {
            //2.获取所有属性中注解为RowKey的属性的注解
            RowKey key = f.getAnnotation(RowKey.class);
            //判断获取到的注解是否为空
            if (key != null) {
                f.setAccessible(true);
                //获取指定列名对应的值
                rowKey = (String) f.get(obj);
                break;
            }
        }

        //1.获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(tableName));

        Put put = new Put(Bytes.toBytes(rowKey));

        //1.1向put中添加数据
        for (Field f : fs) {
            //获取所有属性中注解为Column的属性的注解
            Column column = f.getAnnotation(Column.class);
            //判断获取到的注解是否为空
            if (column != null) {
                //1.2获取列族名
                String family = column.family();

                //1.3获取具体列名
                String columnName = column.column();

                //如果获取到的列名为空，则将属性名作为列名
                if (columnName == null || "".equals(columnName)) {
                    columnName = f.getName();
                }

                //获取指定列名对应的值
                f.setAccessible(true);
                String value = (String) f.get(obj);

                //将获取到的数据添加到put中
                put.addColumn(Bytes.toBytes(family), Bytes.toBytes(columnName), Bytes.toBytes(value));
            }
        }


        //2.增加数据
        table.put(put);

        //3.关闭表
        table.close();

    }

    public void putData(String name, List<Put> puts) throws IOException {
        //获取表对象
        Connection connection = getConnection();
        Table table = connection.getTable(TableName.valueOf(name));

        //增加数据
        table.put(puts);

        //关闭表
        table.close();
    }

}
