package ct.producer.io;

import ct.common.bean.Data;
import ct.common.bean.DataIn;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 本地文件输入
 */
public class LocalFileDataIn implements DataIn {
    private BufferedReader reader = null;

    public LocalFileDataIn(String path){
        setPath(path);
    }

    @Override
    public void setPath(String path) {
        try {
            reader = new BufferedReader(new InputStreamReader(new FileInputStream(path),"UTF-8"));
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public Object read() throws IOException {
        return null;
    }

    @Override
    public <T extends Data> List<T> read(Class<T> clazz) throws IOException {
        List<T> ts = new ArrayList<>();
        //从数据文件中读取所有数据
        String line = null;
        while ((line = reader.readLine()) != null){
            //将数据转化成指定类型的对象
            T t = null;
            try {
                t = clazz.newInstance();
            } catch (InstantiationException e) {
                e.printStackTrace();
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
            t.setValue(line);
            ts.add(t);
        }
        return ts;
    }

    /**
     * 关闭资源
     * @throws IOException
     */
    public void close() throws IOException{
        if(reader != null){
            reader.close();
        }
    }
}
