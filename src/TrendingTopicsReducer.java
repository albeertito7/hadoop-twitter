import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TrendingTopicsReducer extends Reducer<Text, IntWritable, IntWritable,Text> {

}
