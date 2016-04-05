import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Step2Mapper extends Mapper<LongWritable, Text, Text, Text> {
	
//	Output format:	decade baseWord secondWord: count aBaseWordCount
//					decade baseWord secondWord: count bSecondWordCount
	@Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] raw = value.toString().split("\t");
		context.write(new Text(raw[0]), new Text(raw[1]));
	}
}
