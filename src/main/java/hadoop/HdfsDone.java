package hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Test;

import java.io.IOException;


/**
 * Created by 17020751 on 2018/1/29.
 *
 * https://tutorials.techmytalk.com/2014/08/16/hadoop-hdfs-java-api/#more-849
 *
 */
public class HdfsDone {
    /**
     *  创建成功
     *
     * @param args
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        //file system
        FileSystem fileSystem = FileSystem.get(conf);
        Path path = new Path("/yy/wwf.txt");
        fileSystem.create(path);
    }

    /**
     * junit hadoop 2.8.1
     * mkdir
     */
    @Test
    public void testMkdir() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        //file system
        FileSystem fileSystem = FileSystem.get(conf);
        fileSystem.mkdirs(new Path("/yy/hw/"));
    }

    /**
     * file status
     *
     * @throws IOException
     */
    @Test
    public void testFileStatus() throws IOException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://localhost:9000");
        //file system
        FileSystem fileSystem = FileSystem.get(conf);
        FileStatus[] fileStatuses = fileSystem.listStatus(new Path("/"));
        for (FileStatus f : fileStatuses){
            System.out.println("path:   " + f.getPath());
            System.out.println("Owner:   " + f.getOwner());
            System.out.println("Permission:   " + f.getPermission());
            System.out.println("Group:   " + f.getGroup());
        }

    }

}
