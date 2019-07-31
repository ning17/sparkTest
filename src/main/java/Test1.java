import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.ArrayList;
import java.util.List;

public class Test1 {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("Test1").setMaster("local[2]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        List<Integer> list = new ArrayList<Integer>();
        for(int i = 1; i < 10; i++){
            list.add(i);
        }
        JavaRDD<Integer> numRDD = (JavaRDD<Integer>) sc.parallelize(list);
        Integer count = numRDD.reduce(new Function2<Integer, Integer, Integer>() {
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(count);


    }
}
