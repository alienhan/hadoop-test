package hadoop;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

/**
 * hadoop 官网 给的demo
 *
 * 通过mvn 打包使用hadoop jar运行 失败
 *
 * Hadoop 划分工作为任务。有两种类型的任务：
 *   Map 任务 (分割及映射)
 *   Reduce 任务 (重排，还原)
 *
 * 输入与输出
 * Map/Reduce框架运转在<key, value>键值对上，也就是说，框架把作业的输入看为是一组<key, value>键值对，
 * 同样也产出一组 <key, value>键值对做为作业的输出，这两组键值对的类型可能不同。
 * 框架需要对key和value的类(classes)进行序列化操作，因此，这些类需要实现Writable接口。
 * 另外，为了方便框架执行排序操作，key类必须实现 WritableComparable接口。
 *
 * 第一步 : 输入
 *  Hello World Bye World
 *
 * Created by 17020751 on 2018/1/25.
 */
public class WordCount {

    /*
     * 继承Mapper类需要定义四个输出、输出类型泛型：
     * 四个泛型类型分别代表：
     * KeyIn        Mapper的输入数据的Key，这里是每行文字的起始位置（0,11,...）
     * ValueIn      Mapper的输入数据的Value，这里是每行文字
     * KeyOut       Mapper的输出数据的Key，这里是每行文字中的单词"hello"
     * ValueOut     Mapper的输出数据的Value，这里是每行文字中的出现的次数
     *
     * Writable接口是一个实现了序列化协议的序列化对象。
     * 在Hadoop中定义一个结构化对象都要实现Writable接口，使得该结构化对象可以序列化为字节流，字节流也可以反序列化为结构化对象。
     * LongWritable类型:Hadoop.io对Long类型的封装类型
     */
    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            System.out.println("key:" + key + ", value:" + value.toString());
            String line = value.toString();
            StringTokenizer tokenizer = new StringTokenizer(line);//然后，它通过StringTokenizer 以空格为分隔符将一行切分为若干tokens，之后，输出< <word>, 1> 形式的键值对
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
        /**
         * 第二步 : map输出
         * 对于示例中的第一个输入，map输出是：
         < Hello, 1>
         < World, 1>
         < Bye, 1>
         < World, 1>
         *
         * 第三步 : combiner 单个文件汇总(对应单个map结果汇总)
         * WordCount还指定了一个combiner (46行)。因此，每次map运行之后，会对输出按照key进行排序，
         * 然后把输出传递给本地的combiner（按照作业的配置与Reducer一样），进行本地聚合。
             第一个map的输出是：
             < Bye, 1>
             < Hello, 1>
             < World, 2>
         *
         */
    }

    /*
     * 继承Reducer类需要定义四个输出、输出类型泛型：
     * 四个泛型类型分别代表：
     * KeyIn        Reducer的输入数据的Key，这里是每行文字中的单词"hello"
     * ValueIn      Reducer的输入数据的Value，这里是每行文字中的次数
     * KeyOut       Reducer的输出数据的Key，这里是每行文字中的单词"hello"
     * ValueOut     Reducer的输出数据的Value，这里是每行文字中的出现的总次数
     */
    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }

        /*
         *   第四步: 全部结果汇总
         *   Reducer(28-36行)中的reduce方法(29-35行) 仅是将每个key（本例中就是单词）出现的次数求和。
             因此这个作业的输出就是：
             < Bye, 1>
             < Goodbye, 1>
             < Hadoop, 2>
             < Hello, 2>
             < World, 2>
         *
         *
         */
    }



    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(WordCount.class);
        conf.setJobName("wordcount");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        //WordCount还指定了一个combiner (46行)。因此，每次map运行之后，会对输出按照key进行排序，
        // 然后把输出传递给本地的combiner（按照作业的配置与Reducer一样），进行本地聚合。
        //用户可选择通过 JobConf.setCombinerClass(Class)指定一个combiner，它负责对中间过程的输出进行本地的聚集，这会有助于降低从Mapper到 Reducer数据传输量。
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        // 设置最后输出结果的Key和Value的类型
        //Hadoop Map/Reduce框架为每一个InputSplit产生一个map任务，而每个InputSplit是由该作业的InputFormat产生的。
        conf.setInputFormat(TextInputFormat.class);//Mapper(14-26行)中的map方法(18-25行)通过指定的 TextInputFormat(49行)一次处理一行。
        conf.setOutputFormat(TextOutputFormat.class);

        //代码中的run方法中指定了作业的几个方面， 例如：通过命令行传递过来的输入/输出路径、key/value的类型、输入/输出的格式等等JobConf中的配置信息。
        // 随后程序调用了JobClient.runJob(55行)来提交作业并且监控它的执行。

        //hdfs://localhost:9000/usr/joe/wordcount/input hdfs://localhost:9000/usr/joe/wordcount/output3
        FileInputFormat.setInputPaths(conf, new Path("hdfs://localhost:9000/usr/joe/wordcount/input"));// 数据HDFS文件服务器读取数据路径
        FileOutputFormat.setOutputPath(conf, new Path("hdfs://localhost:9000/usr/joe/wordcount/output5/"));// 将计算的结果上传到HDFS服务

        // 执行提交job方法
        JobClient.runJob(conf);
        System.out.println("Finished");
    }
}
