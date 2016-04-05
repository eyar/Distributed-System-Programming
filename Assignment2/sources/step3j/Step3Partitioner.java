import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Step3Partitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text Value, int numPartitions) {
            String[] e = Value.toString().split(" ");
            return Integer.parseInt(e[0]) - 190;
        }
}
