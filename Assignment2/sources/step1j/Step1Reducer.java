import java.io.IOException;
import java.util.Arrays;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Step1Reducer extends Reducer<Text, Text, Text, Text> {
	String[] lastKey;
	private long lastWordCount, lastPairCount;
	
	@Override
    public void setup(Context context) throws IOException, InterruptedException {
		lastWordCount = 0;
		lastPairCount = 0;
		lastKey = new String[] {""};
	}
	
//	Output format:		decade baseWord secondWord: count aBaseWordCount
//						decade baseWord secondWord: count bSecondWordCount	
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
		String[] rawKey = key.toString().split(" ");
		if(lastKey.length==1) {	// First Iteration
			lastKey = rawKey;
			if(lastKey.length==3) lastWordCount = Long.parseLong(values.iterator().next().toString().split(" ")[0]);
			else if(lastKey.length==4) lastPairCount = Long.parseLong(values.iterator().next().toString().split(" ")[0]);
		}
		if(!Arrays.equals(lastKey, rawKey)){
			if(lastKey.length==4){	// Last key is original pair
				Text pairCountWordCount = new Text(lastPairCount + " " + "a" + lastWordCount); 
				Text decadePair = new Text(lastKey[0] + " " + lastKey[1] + " " + lastKey[2]);
				context.write(decadePair, pairCountWordCount);
			}
			else if(lastKey.length==5){	// Last key is reversed pair
				Text pairCountWordCount = new Text(lastPairCount + " " + "b" + lastWordCount); 
				Text decadePair = new Text(lastKey[0] + " " + lastKey[2] + " " + lastKey[1]);
				context.write(decadePair, pairCountWordCount);
			}
//			This is for the current key
			if(rawKey.length==3) lastWordCount = Long.parseLong(values.iterator().next().toString().split(" ")[0]);
			else if(rawKey.length==4) lastPairCount = Long.parseLong(values.iterator().next().toString().split(" ")[0]);
			else if(rawKey.length==5) lastPairCount = Long.parseLong(values.iterator().next().toString().split(" ")[0]);
			lastKey = rawKey;
		}
//		If more then one value, sum up
		if(rawKey.length==3){
			for(Text t: values){
				lastWordCount += Long.parseLong(t.toString().split(" ")[0]);
			}
		}
		else if(rawKey.length==4){
			for(Text t: values){
				lastPairCount += Long.parseLong(t.toString().split(" ")[0]);
			}
		}
		else if(rawKey.length==5){	// This is the same as previous loop, for double checking
			for(Text t: values){
				lastPairCount += Long.parseLong(t.toString().split(" ")[0]);
			}
		}
    }
	
//	Write last key and value to context
	@Override
    public void cleanup(Context context) throws IOException, InterruptedException {
		if(lastKey.length==4){
			Text pairCountWordCount = new Text(lastPairCount + " " + "a" + lastWordCount); 
			Text decadePair = new Text(lastKey[0] + " " + lastKey[1] + " " + lastKey[2]);
			context.write(decadePair, pairCountWordCount);
		}
		else if(lastKey.length==5){
			Text pairCountWordCount = new Text(lastPairCount + " " + "b" + lastWordCount); 
			Text decadePair = new Text(lastKey[0] + " " + lastKey[2] + " " + lastKey[1]);
			context.write(decadePair, pairCountWordCount);
		}
	}
}
