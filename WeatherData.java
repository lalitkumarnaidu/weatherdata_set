import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class WeatherData {
    public static class OMapper extends Mapper<LongWritable,Text,Text,Text>{
        @Override
        public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
            /*Get input record line and Normalize*/
            String tvalue = value.toString();
            tvalue = tvalue.replaceAll("[\\s,]+", " ");
	        String delims= "[ ]";
            String[] strList = tvalue.split(delims);
            /*Validate for file header*/
             if(!(strList[0].equals("STN---") || strList[3].equals("TEMP"))){
            String StationID = strList[0];
            String yearmode = strList[2];
            String MeanWindSpeed ="";
            String temperature = "";
            /*Validate missing weather data and normalize output value*/
            if(!(Float.valueOf(strList[3]) >= 999.9))
            {
             temperature = strList[3];
            }
            else
            {temperature = "0";}

            if(!(strList[12].equals("999.9")))
            {MeanWindSpeed = strList[12];}
            else
            {MeanWindSpeed = "0";}
            String[] yearmodeList = yearmode.split("");
            String yearmonth= yearmodeList[1]+yearmodeList[2]+yearmodeList[3]+yearmodeList[4]+":"+yearmodeList[5] + yearmodeList[6];
            String stnyearmonth = StationID+" "+yearmonth;
            String tempandWSDP = temperature+" "+MeanWindSpeed;
            c.write(new Text(stnyearmonth),new Text(tempandWSDP));
               }
            }   
        }
        
    
    public static class OReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{
            float maxtemp = Float.MIN_VALUE;
            float minWSDP =Float.MAX_VALUE;
            float maxWSDP = Float.MIN_VALUE;
            float mintemp =Float.MAX_VALUE;
            float avgWSDP = 0;
            float avgtemp= 0;
            int wcnt =0;
            int tcnt =0;
            int cnt =0;
            String AvgTemp ="";
            String AvgWSDP ="";
            for(Text val:values)
            {
                cnt++;
                String[]  valList = (val.toString()).split(" ");
                float temp = Float.valueOf(valList[0]);
                float WSDP = Float.valueOf(valList[1]);
                if(!(temp <= 0))
                 {
                  
                    maxtemp = Math.max(maxtemp,temp);
                    mintemp = Math.min(mintemp,temp);
                    avgtemp = avgtemp + temp;
                    
                  }
               else
                 tcnt++;

                if(!(WSDP <= 0))
                {
                  maxWSDP = Math.max(maxWSDP,WSDP);
                  minWSDP = Math.min(minWSDP,WSDP);
                  avgWSDP = avgWSDP + WSDP;
                }
                else
                wcnt++;
                
            }
                if(avgtemp != 0){
                  avgtemp = avgtemp/(cnt-tcnt);
                  AvgTemp +=avgtemp;
                }
                else {
                  AvgTemp = "Missing";
                }

                if(avgWSDP != 0){
                  avgWSDP = avgWSDP/(cnt-wcnt);
                  AvgWSDP +=avgWSDP;
                }
                 else
                {
                  AvgWSDP = "Missing";
                }
                String val = maxtemp+" "+mintemp+" "+AvgTemp+" "+maxWSDP+" "+minWSDP+" "+AvgWSDP;
            c.write(key, new Text(val));
        }
    }
    
    


public static class PMapper extends Mapper<LongWritable,Text,Text,Text>{
    
      public void map(LongWritable key, Text value, Context c) throws IOException, InterruptedException{
      String tvalue = value.toString();
      tvalue = tvalue.replaceAll("[\\t]+"," ");
	  String delims= "[ ]";
      String[] strList = tvalue.split(delims);
      String Station = strList[0];
      String[] Yearmonth = strList[1].split(":"); 
      String Year = Yearmonth[0];  
     String Value=  Station+" "+strList[2]+" "+strList[3]+" "+strList[5]+" "+strList[6]+",";
       c.write(new Text(Year),new Text(Value));
    }
    
 }

  public static class PReducer extends Reducer<Text,Text,Text,Text>{
        @Override
        public void reduce(Text key, Iterable<Text>values, Context c) throws IOException,InterruptedException{
        	int values_length =0;
                String tvalue ="";
      
             for(Text val:values)       
             {
                tvalue +=val.toString();
                 ++values_length;
             }
            String[] vmaxtemp = new String[values_length];
            String[] vmintemp = new String[values_length];
            String[] vavgws = new String[values_length];
            String [] Station = new String[values_length];
            String[] vavgwscp = new String[values_length];
            String[] temp = tvalue.split(",");
            
           //String [] StationOUT = new String[4];
           String MINTOUT ="";
           String MAXTOUT ="";
           String MAXWSOUT ="";
           String MINWSOUT ="";
            int i=0;
             for(String val: temp)
            {
               String[]  valList = val.split(" ");
                Station[i] = valList[0];
                vmaxtemp[i] = valList[1];
                vmintemp[i] = valList[2];
                vavgws[i] = valList[3];
                vavgwscp[i] = valList[4];
                 ++i;
            }
                  
            for(int k =0 ; k<5;k++){
            int cntMAXT =0;
            int cntMINT =0;
            int cntMAXWS =0;
            int cntMINWS =0;
            float maxtemp = Float.MIN_VALUE;
            float mintemp =Float.MAX_VALUE;
            float maxavgws= Float.MIN_VALUE;
            float minavgws= Float.MAX_VALUE;
            float cmpMINT =Float.MAX_VALUE;
            float cmpMAXT =Float.MIN_VALUE;
            float cmpMINWS = Float.MAX_VALUE;
            float cmpMAXWS = Float.MIN_VALUE;
             for(int j=0; j<values_length ; j++)
            {
                if(!(vmaxtemp[j].equals("Missing"))){
                   maxtemp = Math.max(maxtemp,Float.valueOf(vmaxtemp[j]));
                   if(cmpMAXT< maxtemp)
		           {
			         cmpMAXT = maxtemp;
			         cntMAXT =j;
			         //StationOUT[0] = Station[j];
		           }
                
                 }
               
                if(!(vmintemp[j].equals("Missing"))){
                mintemp = Math.min(mintemp,Float.valueOf(vmintemp[j]));
                if(cmpMINT > mintemp)
		           {
			         cmpMINT = mintemp;
			         cntMINT =j; 
			         // StationOUT[1] = Station[j];
		           }
                
                }
               
                if(!(vavgws[j].equals("Missing")))
                {
                maxavgws = Math.max(maxavgws,Float.valueOf(vavgws[j]));
                if(cmpMAXWS< maxavgws)
		           {
			         cmpMAXWS = maxavgws;
			         cntMAXWS =j; 
			        // StationOUT[2]= Station[j];
		           }
                }

               if(!(vavgwscp[j].equals("Missing"))){
                minavgws = Math.min(minavgws,Float.valueOf(vavgwscp[j]));
                if(cmpMINWS > minavgws)
		           {
			         cmpMINWS = minavgws;
			         cntMINWS =j; 
			        //StationOUT[3] = Station[j];
		           }
                }
               
            }
            
            MAXTOUT += "( "+(k+1)+"="+Station[cntMAXT]+","+ maxtemp+" ),";
            MINTOUT += "( "+(k+1)+"="+Station[cntMINT]+","+ mintemp+" ),";
            MAXWSOUT +="( "+(k+1)+"="+Station[cntMAXWS]+","+maxavgws+" ),";
           MINWSOUT +="( "+(k+1)+"="+Station[cntMINWS]+","+ minavgws+" ),";
            vmaxtemp[cntMAXT] = String.valueOf(Float.MIN_VALUE);
            vmintemp[cntMINT] = String.valueOf(Float.MAX_VALUE);
            vavgws[cntMAXWS] = String.valueOf(Float.MIN_VALUE);
            vavgwscp[cntMINWS] = String.valueOf(Float.MAX_VALUE);
        }
             String Output = "MaxTemp{ "+MAXTOUT +"} MinTemp{ "+MINTOUT+" } MaxWSDP{ "+MAXWSOUT+" } MinWSDP{ "+MINWSOUT+" }";
            c.write(key, new Text(Output));
  
        }
    } 
  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
    Configuration conf = new Configuration();
       /*Job 1 Starts here*/
       Job j1 = new Job(conf,"Job One");
	    j1.setJarByClass(WeatherData.class);
	    j1.setMapOutputKeyClass(Text.class);
	    j1.setMapOutputValueClass(Text.class);
        /** set number of reducer**/
	    //j1.setNumReduceTasks(4);
        j1.setOutputKeyClass(Text.class);
        j1.setOutputValueClass(Text.class);
        j1.setInputFormatClass(TextInputFormat.class);
        j1.setOutputFormatClass(TextOutputFormat.class);
        j1.setMapperClass(OMapper.class);
        j1.setReducerClass(OReducer.class);
        FileOutputFormat.setOutputPath(j1, new Path(args[1]));
        FileInputFormat.addInputPath(j1, new Path(args[0]));
        //FileInputFormat.setMaxInputSplitSize(j1,54525952);
        j1.waitForCompletion(true);
          
       /*Job 2 Starts here*/
       Job j2 = new Job(conf,"Job Two");
	    j2.setJarByClass(WeatherData.class);
	    j2.setMapOutputKeyClass(Text.class);
	    j2.setMapOutputValueClass(Text.class);
	    j2.setOutputKeyClass(Text.class);
	    j2.setOutputValueClass(Text.class);
	    j2.setInputFormatClass(TextInputFormat.class);
        j2.setOutputFormatClass(TextOutputFormat.class);
        /** set number of reducer**/
	    // j2.setNumReduceTasks(4);
	    j2.setMapperClass(PMapper.class);
	    j2.setReducerClass(PReducer.class);
        FileOutputFormat.setOutputPath(j2, new Path(args[2]));
        FileInputFormat.addInputPath(j2, new Path(args[1]));
        //FileInputFormat.setMaxInputSplitSize(j2,54525952);
        j2.waitForCompletion(true);
  }
}
