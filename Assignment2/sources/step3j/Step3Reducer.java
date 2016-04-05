import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step3Reducer extends Reducer<Text, Text, Text, Text> {
	private long k;
	private long i;
	String lastMeasure;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		Configuration conf = context.getConfiguration();
		k = Long.parseLong(conf.get("k"));
		i = 0;
    }
	
//	Output format:	measure-serialNum: decade pair jointP jointPValue dice diceValue geoMean geoMeanValue
	@Override
	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] rawKey = key.toString().split(" ");
		if(lastMeasure==null || !lastMeasure.equals(rawKey[0])){
			i = 0;
			lastMeasure = rawKey[0]; 
		}
		for(Text t: values){
			if(i<k){
				String[] s = t.toString().split(" ");
				Text V = new Text(s[0] + " " + s[1] + " " + s[2] + "\t" + s[3] + " " + s[4] + "\t" + s[5] + " " + s[6] + "\t" + s[7] + " " + s[8]);
				context.write(new Text(rawKey[0] + "-" + i), V);
				i++;
			}
		}
	}
}
