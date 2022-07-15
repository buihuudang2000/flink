package jar;


import java.io.*;
import java.util.*;

import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;



public class WriteMetric {
    static long MinEventTimestamp = Long.MAX_VALUE ;
    static long MaxEventTimestamp = 0;
    static int flag = 0;
    static List<String> metricArray = new ArrayList<String>();
    static void  writeFile(String path, List<String> contents) throws IOException {
        System.out.println(path);
        File file = new File(path);
        FileOutputStream fos = new FileOutputStream(file);
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fos));
        for (String i:contents) {
            bw.write(i);
            bw.newLine();
        }
        bw.close();
    }
    static String collectMetricToFile(String data) throws JSONException, IOException {

        JSONObject json =  new JSONObject(data);
        // get timestamp from metric
        String str= json.getJSONObject("metric").getString("timestamp");
        // convert int to datetime
        long timestamp = Long.parseLong(str);
        Date d = new Date(timestamp );

        String dir = "/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/output/v1/";
        int day = d.getDay()+10;
        int month= d.getMonth();
        int year= d.getYear() + 1900;
        String namefile= String.valueOf(System.currentTimeMillis()) +"_" +String.valueOf(MinEventTimestamp) +"_"+ String.valueOf(MaxEventTimestamp) + ".parquet";

        if (flag == 0) {
            flag = day;
            MinEventTimestamp = timestamp<MinEventTimestamp ? timestamp: MinEventTimestamp;
            MaxEventTimestamp = timestamp>MaxEventTimestamp ? timestamp : MaxEventTimestamp;
            metricArray.add(data);
        } else if (day != flag ) {
            File path = new File(dir+ String.valueOf(year));
            if (!path.exists()) {
                path.mkdir();
            }
            path = new File(dir+ String.valueOf(year) +"/"+ String.valueOf(month));
            if (!path.exists()) {
                path.mkdir();
            }
            writeFile(dir+ String.valueOf(year) +"/"+ String.valueOf(month) + "/" + namefile, metricArray);
//            System.out.print(namefile);
//            System.out.println(metricArray);
            flag = day;
            metricArray = new ArrayList<String>();

        } else {
            MinEventTimestamp = timestamp<MinEventTimestamp ? timestamp: MinEventTimestamp;
            MaxEventTimestamp = timestamp>MaxEventTimestamp ? timestamp : MaxEventTimestamp;
            metricArray.add(data);
        }

        return dir+ String.valueOf(year) +"/"+ String.valueOf(month) + "/" + namefile;
    }
    static void readFile(){
        try {
            File myObj = new File("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/jar/input.txt");
//            File myObj = new File("/home/lap12949/Documents/fresher/final-project/writemetric/src/input/input.txt");
            Scanner myReader = new Scanner(myObj);
            String path="";
            while (myReader.hasNextLine()) {
                String data = myReader.nextLine();
                path=collectMetricToFile(data);
            }
            writeFile(path, metricArray);
            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (JSONException e) {
            throw new RuntimeException(e);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    public static void main(String[] args) {
        readFile();
    }
}


