package hdfs;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import org.apache.commons.compress.utils.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.util.Progressable;

public class FileCopyWithProgress{
    public static void main(String[] args) throws IOException {
        System.setProperty("HADOOP_USER_NAME","jinzhao");
        String localsrc = "G:\\test\\log1.txt";
        String dst = "hdfs://192.168.10.156:9000/test/input/log1.txt";
        InputStream in = new BufferedInputStream(new FileInputStream(localsrc));

        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst),conf);

        try (OutputStream out = fs.create(new Path(dst),new Progressable(){
            public void progress()
            {
                System.out.print(".");//用于显示文件复制进度
            }
        }))
        {
            IOUtils.copy(in, out);
        }
    }
}