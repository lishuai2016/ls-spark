package ls.spark.core;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

/**
 * 从本地读取 写入到 HDFS 上
 */
public class JavaLocalFileUploadHdfs {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.err.println("Usage: JavaLocalFileUploadHdfs <localFile> <dfsDir>");
            System.exit(1);
        }
        String localFile = args[0];
        String dfsDir = args[1];

        SparkConf sparkConf = new SparkConf().setAppName("JavaLocalFileUploadHdfs");
        JavaSparkContext jsc = new JavaSparkContext(sparkConf);

        List<String> lineList = readLocalFile(localFile);

        if (!lineList.isEmpty()) {
            printList("SparkDemo-->>JavaLocalFileUploadHdfs-->>The local file's content is :", lineList);
            jsc.parallelize(lineList).saveAsTextFile(dfsDir);

            // 读取hdfs中的文件
            JavaRDD<String> lines = jsc.textFile(dfsDir);
            printList("SparkDemo-->>JavaLocalFileUploadHdfs-->>The hdfs file's content is :", lines.collect());
        } else {
            System.out.println("Error: The local file is Empty!");
        }

        jsc.close();
    }

    /**
     * 读取本地文件（直接）
     * @param localPath
     */
    private static List<String> readLocalFile(String localPath) {
        List<String> lineList = new ArrayList<String>();
        BufferedReader reader = null;
        try {
            reader = new BufferedReader(new FileReader(new File(localPath)));
            String temp = null;
            // 一次读一行，读入null时文件结束
            while ((temp = reader.readLine()) != null) {
                lineList.add(temp);
            }
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (reader != null) {
                try {
                    reader.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
        return lineList;
    }

    /**
     * 打印list内容
     * <p>
     *
     * @param title
     * @param lineList
     */
    private static void printList(String title, List<String> lineList) {
        System.out.println(title);
        for (String line : lineList) {
            System.out.println(line);
        }
    }
}