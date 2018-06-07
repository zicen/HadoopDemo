import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map;


public class HdfsClientDemo {
    FileSystem fs = null;
    private Configuration configuration;

    @Before
    public void init() throws Exception{
        configuration = new Configuration();
        configuration.set("fs.defaultFS","hdfs://mini:9000");
        fs = FileSystem.get(new URI("hdfs://mini:9000"), configuration, "root");
    }

    /**
     * 上传本地文件到HDFS服务器
     * @throws Exception
     */
    @Test
    public void testUpload() throws Exception{
        Thread.sleep(3000);
        fs.copyFromLocalFile(new Path("E:/book4.txt"),new Path("/testUpload.log"));
        fs.close();
    }

    @Test
    public void testConf(){
        Iterator<Map.Entry<String, String>> iterator = configuration.iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, String> entry = iterator.next();
            System.out.println(entry.getKey() + "--" + entry.getValue());
        }
    }

    /**
     * 创建目录
     */
    @Test
    public  void mkdirTest() throws IOException {
        boolean mkdirs = fs.mkdirs(new Path("/aaa/bbbb"));
        System.out.println(mkdirs);
    }

    /**
     * 删除
     * @throws Exception
     */
    @Test
    public void deleteTest() throws Exception{
        boolean delete = fs.delete(new Path("/aaa"), true);
        System.out.println(delete);
    }

    /**
     * 递归列出所有的文件
     * @throws Exception
     */
    @Test
    public void listFileTest() throws Exception{
        RemoteIterator<LocatedFileStatus> locatedFileStatusRemoteIterator = fs.listFiles(new Path("/"), true);
        while (locatedFileStatusRemoteIterator.hasNext()) {
            LocatedFileStatus next = locatedFileStatusRemoteIterator.next();
            System.out.println(next.getPath().getName()+"---"+next.getPath());
        }
    }

}
