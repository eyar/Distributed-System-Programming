import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Step2Partitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text Value, int numPartitions) {
            String[] e = key.toString().split(" ");
            return Integer.parseInt(e[0]) - 190;
        }
}
