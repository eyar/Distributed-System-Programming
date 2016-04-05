import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;

public class Step2Reducer extends Reducer<Text, Text, Text, Text> {
	private double totalWordsInDecade;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		totalWordsInDecade = 0;
	}
	
//	Output format:	measure measureValue: decade pair jointP jointPValue dice diceValue geoMean geoMeanValue
	@Override
	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] rawKey = key.toString().split(" ");
		double pairCount = 0;
		double[] wordCount = new double[2];
		for(Text t: values){
			String[] counts = t.toString().split(" ");
			if(counts[1].charAt(0)=='a'){
				totalWordsInDecade++;
				wordCount[0] = Double.parseDouble(counts[1].split("a")[1]);	// Single first word count
				pairCount = Double.parseDouble(counts[0]);
			}
			else wordCount[1] = Double.parseDouble(counts[1].split("b")[1]);	// Single second word count
		}
		double jointP = pairCount/totalWordsInDecade;
		double dice = 2d*pairCount/(wordCount[0]+wordCount[1]);
		double geoMean = Math.sqrt(dice*jointP);
		String joinPKey = "jointP " + String.format("%.25f", jointP);
		String diceKey = "dice " + String.format("%.25f", dice);
		String geoMeanKey = "geoMean " + String.format("%.25f", geoMean);
		Text rest = new Text(key.toString() + " " + joinPKey + " " + diceKey + " " + geoMeanKey);
		context.write(new Text(joinPKey), rest);
		context.write(new Text(diceKey), rest);
		context.write(new Text(geoMeanKey), rest);
	}
}
