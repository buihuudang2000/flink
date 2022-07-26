package jar.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import jar.config.AmazonS3ConfigToDownload;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.io.*;
import java.util.*;

public class ReadMetric {
    private static String getTextInputStream(InputStream input) throws IOException {
        // Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        String data="";
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;
            data += line;
//            System.out.println("    " + line);
        }
        return data;
    }
    private static List<String> getTextInputStream2(InputStream input) throws IOException {
        // Read one text line at a time and display.
        BufferedReader in = new BufferedReader(new InputStreamReader(input));
//        FileReader in = new FileReader("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/jar/input.log");
        List<String> contents=new ArrayList<>();
        char[] chars = new char[1000000];
//            int n = in.read(chars, 0, chars.length);
//        String contents ="";
        String data="";

        while (true) {
            int n= in.read(chars, 0, chars.length);
            System.out.println(n);

            if (n== -1)
                break;
            if (n< chars.length)
                contents.add( new String(chars).substring(0,n));
            else
                contents.add( new String(chars));
//            System.out.println("    " + line);

        }

//        System.out.println(contents);
        return contents;
    }
    public static DataSet<Tuple6<String, Long, Double, String, String, Integer>> convertToDataset(List<String> list, ExecutionEnvironment env, long timestampForItem){
        try {
            JSONObject json;
            JSONObject dimensionsJson;
            String dimensions="";
            String keyvalue="";
            String tenantId= "";
            List<Tuple6<String, Long, Double, String, String, Integer>> arr=new ArrayList<>();
            int size= list.size();
            for (int i=0; i<size-1; i++){
                dimensions="";
                String item= list.get(i);
                item = item.substring(1);
//                System.out.println(item);
//              {"headers":{},"message":"{\"metric\":{\"dimensions\":{\"product\":\"vserver\",\"hostname\":\"server2\",\"zone\":\"HCM-02\",\"user_id\":11323,\"resource_id\":\"b7c6c707-03cd-416c-8949-73cf69723775\",\"device\":\"sda\"},\"name\":\"vserver.io.read_ops\",\"timestamp\":1657856138989,\"value\":26793.0,\"value_meta\":{}},\"meta\":{\"tenantId\":\"ab0df5851da44e22b97c44e977bb9cb8\",\"region\":\"RegionOne\"},\"creation_time\":1657856138989}","message_key":null,"offset":1518254536,"partition":0,"source_type":"kafka","timestamp":"2022-07-15T03:35:41.376Z",
                json= new JSONObject(item + "\"temp\":\"\"}");
                json= (new JSONObject(json.getString("message")));

                tenantId= json.getJSONObject("meta").getString("tenantId");
                // get matric in message
                json= json.getJSONObject("metric");

                //get dimensions in matric
                dimensionsJson= json.getJSONObject("dimensions");

                for (Iterator key = dimensionsJson.keys(); key.hasNext();) {
                    keyvalue= key.next().toString();
                    dimensions = dimensions + keyvalue+ ":" + dimensionsJson.getString(keyvalue)+",";

                    //now name contains the firstname, and so on...
                }
                dimensions=dimensions.substring(0,dimensions.length()-1);
                System.out.println(dimensions);
//
//                System.out.println(tenantId);
//                System.out.println(json);
                arr.add(new Tuple6<String, Long, Double, String, String, Integer>(json.getString("name"),timestampForItem,json.getDouble("value"),tenantId,dimensions,1));
//                arr.add(new Tuple6<String, Long, Double, String, String, Integer>(json.getString("name"),json.getLong("timestamp"),json.getDouble("value"),tenantId,dimensions,1));

            }
            DataSet<Tuple6<String, Long, Double, String, String, Integer>> data= env.fromCollection(arr);
            return data;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    public static List<String> readFile2(S3Object objectPortion){
        try {

            List<String> data=getTextInputStream2(objectPortion.getObjectContent());
            List<String> myList=new ArrayList<>();
//            System.out.println(data);
            String preitem="";
            List<String> myListTemp=new ArrayList<>();
            for (int i=0; i<data.size();i++){
                String item= data.get(i);
                myListTemp= new ArrayList<String>(Arrays.asList(item.split("\"topic\":\"metrics\"}") ));
                myListTemp.set(0,preitem+myListTemp.get(0));

                preitem=myListTemp.get(myListTemp.size()-1);
                myList.addAll(myListTemp.subList(0,myListTemp.size()-1));

//                System.out.println(myList.get(myList.size()-1));
            }
            myList.add(myListTemp.get(myListTemp.size()-1));
//            List<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            Collector<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            DataSet<List<String>> myList = env.fromElements(new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") )) );
            int size = myList.size();
//            myList.set(0, myList.get(0).substring(1));
//            System.out.println(myList);
            return myList;

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    public static List<String> readFile(S3Object objectPortion){
        try {

            String data=getTextInputStream(objectPortion.getObjectContent());
//            System.out.println(data);

            List<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            Collector<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            DataSet<List<String>> myList = env.fromElements(new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") )) );
            int size = myList.size();
//            myList.set(0, myList.get(0).substring(1));

            return myList;

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
    public static List<String> readFile02(GetObjectRequest rangeObjectRequest){
        try {
            AmazonS3 s3= AmazonS3ConfigToDownload.s3client();
            S3Object objectPortion = s3.getObject(rangeObjectRequest);
            String data=getTextInputStream(objectPortion.getObjectContent());
//            System.out.println(data);
            objectPortion.close();
            List<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            Collector<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            DataSet<List<String>> myList = env.fromElements(new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") )) );
//            int size = myList.size();
//            myList.set(0, myList.get(0).substring(1));

            return myList;

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return null;
    }
}
