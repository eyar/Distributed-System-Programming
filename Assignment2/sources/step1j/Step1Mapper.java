import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;


public class Step1Mapper extends Mapper<LongWritable, Text, Text, Text> {
	private static final String stopWordsUrl = "http://web.archive.org/web/20080501010608/http://www.dcs.gla.ac.uk/idom/ir_resources/linguistic_utils/stop_words";
    ArrayList<String> stopWords = new ArrayList<>();

    protected void setup(Context context) throws IOException, InterruptedException {
        extractStopWords();
    }

//	Output format: 		decade baseWord *  : count
//						decade secondWord *: count
//						decade baseWord secondWord *  : count
//						decade secondWord baseWord * *: count
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
    	if (value.toString().length() > 0) {
            String[] e = value.toString().split("\t");
            if(Integer.parseInt(e[1].substring(0, 3))>=190){
	            String[] words = e[0].split(" ");
	            for(int i=0;i<words.length;i++){
	            	words[i] = words[i].toLowerCase();
	            }
	            if(words.length==5){
            	words[2] = shave(words[2]);
            	if(words[2].isEmpty() || stopWords.contains(words[2])) return;
	            	for(int i=0;i<5;i++){
	            		if(i!=2){
	            			try{
	        					words[i] = shave(words[i]);
	        					if(words[i].isEmpty() || stopWords.contains(words[i])) continue;
	        					String decade = e[1].substring(0, 3);
	        					Text decadePair = new Text(decade + " " + words[2] + " " + words[i] + " * ");
	        					Text decadeReversePair = new Text(decade + " " + words[i] + " " + words[2] + " * *");
	        					Text count = new Text(e[2]);
	        					context.write(new Text(decade + " " + words[2] + " *"), count);
	        					context.write(new Text(decade + " " + words[i] + " *"), count);
	        					context.write(decadePair, count);
	        					context.write(decadeReversePair, count);
	            			}catch(Exception ex){
	            				System.out.println("key is "+key);
	            				ex.printStackTrace(System.err);
	            				break;
	            			}
	            		}
	            	}
	            }
	    	}
    	}
	}
    
//  This is the filtering function
    public static String shave(String word){
		if(word.isEmpty() || (word.length()==1 && !word.matches("^[a-z]*$"))) return ""; // Remove empty or single no alphabetic strings 
		else if(word.length()==1 && word.matches("^[a-z]*$"))  return word;
//		Remove symbols from head or tail of word
    	String[] firstAndLastChar = {word.substring(0,1),word.substring(word.length()-1,word.length())}; 
    	while(!(firstAndLastChar[0] + firstAndLastChar[1]).matches("^[a-z0-9]*$")){
	    	if(!firstAndLastChar[1].matches("^[a-z0-9]*$")){
	    		word = word.substring(0, word.length()-1);
			}
	    	if(!firstAndLastChar[0].matches("^[a-z0-9]*$")){
				word = word.substring(1, word.length());
			}
	    	if(word.isEmpty() || (word.length()==1 && !word.matches("^[a-z]*$"))) return "";
			else if(word.length()==1 && word.matches("^[a-z]*$"))  return word;
	    	firstAndLastChar[0] = word.substring(0,1);
	    	firstAndLastChar[1] = word.substring(word.length()-1,word.length());
    	}
//    	Make sure word contains letters
    	Pattern p = Pattern.compile("[a-z]");
    	Matcher m = p.matcher(word);
    	if(m.find()){
    		if(word.contains("'s")) return word.substring(0,word.length()-2);
    		else return word;
    	}
    	else return "";
    }
    
    private void extractStopWords() {
    	try {
            URL website = new URL(stopWordsUrl);
            ReadableByteChannel rbc = Channels.newChannel(website.openStream());
            FileOutputStream fos = new FileOutputStream("stop_words");
            fos.getChannel().transferFrom(rbc, 0, Long.MAX_VALUE);
            fos.close();

            // Extract stop Words From File
            BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream("stop_words"), "UTF-8"));
            String line;

            while ((line = reader.readLine()) != null) {
                stopWords.add(line);
            }

            reader.close();
        } catch (Exception e) {
            e.printStackTrace();
        }		
	}
}
