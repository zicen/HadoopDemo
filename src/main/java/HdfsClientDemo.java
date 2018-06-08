import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
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

    /**
     * 获取一个文件的所有block位置信息，然后读取指定block中的内容
     * @throws IllegalArgumentException
     * @throws IOException
     */
    @Test
    public void testCat() throws IllegalArgumentException,IOException{
        FSDataInputStream in = fs.open(new Path("/wordcount/input/book1.txt"));
        //拿到文件信息
        FileStatus[] fileStatuses = fs.listStatus(new Path("/wordcount/input/book1.txt"));
        //获取这个文件的所有block的信息
        BlockLocation[] fileBlockLocations = fs.getFileBlockLocations(fileStatuses[0], 0L, fileStatuses[0].getLen());
        //第一个block的长度
        long length = fileBlockLocations[0].getLength();
        //第一个block的起始偏移量
        long offset = fileBlockLocations[0].getOffset();

        System.out.println(length);
        System.out.println(offset);
        //获取第一个block的写入输出流
        IOUtils.copyBytes(in, System.out, (int) length);
        byte[] b = new byte[4096];

        FileOutputStream os = new FileOutputStream(new File("d:/block0.txt"));
        while (in.read(offset, b, 0, 4096) != -1) {
            os.write(b);
            offset += 4096;
            if (offset>=length) return;
        }
        os.flush();
        os.close();
        in.close();
    }
}
