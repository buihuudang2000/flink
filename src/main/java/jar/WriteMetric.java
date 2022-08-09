package jar;


import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.util.*;

import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import jar.config.AmazonS3ConfigToUpload;
import jar.service.DownloadService;
import jar.service.ReadMetric;
import jar.service.UploadService;
import jar.service.WriteToParquet;
import jdk.nashorn.internal.parser.DateParser;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.operators.Order;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import static jdk.jfr.internal.SecuritySupport.getResourceAsStream;


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
            metricArray.add(data);
            MinEventTimestamp= Long.MAX_VALUE;
        } else {
            MinEventTimestamp = timestamp<MinEventTimestamp ? timestamp: MinEventTimestamp;
            MaxEventTimestamp = timestamp>MaxEventTimestamp ? timestamp : MaxEventTimestamp;
            metricArray.add(data);
        }

        return dir+ String.valueOf(year) +"/"+ String.valueOf(month) + "/" + namefile;
    }
    static void readFile(){
        try {
//            File myObj = new File("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/jar/input.txt");
////            File myObj = new File("/home/lap12949/Documents/fresher/final-project/writemetric/src/input/input.txt");
//            Scanner myReader = new Scanner(myObj);
//            String path="";

            FileReader in = new FileReader("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/jar/input.log");

            char[] chars = new char[1000000];
//            int n = in.read(chars, 0, chars.length);
            String contents ="";
            String data="";

            while (true) {
                int n= in.read(chars, 0, chars.length);
                System.out.println(n);

                if (n== -1)
                    break;
                if (n< chars.length)
                    contents += new String(chars).substring(0,n);
                    else
                    contents +=  new String(chars);
//            System.out.println("    " + line);

            }

            System.out.println(contents);
//            while (myReader.hasNextLine()) {
//                String data = myReader.nextLine();
//                path=collectMetricToFile(data);
//            }
//            writeFile(path, metricArray);
//            myReader.close();
        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }
    static DataSet<Tuple5<String, Long, Double, String, String>> convertToDataset(List<String> list, ExecutionEnvironment env){
        try {
            JSONObject json;
            JSONObject dimensionsJson;
            String dimensions="";
            String keyvalue="";
            String tenantId= "";
            List<Tuple5<String, Long, Double, String, String>> arr=new ArrayList<>();
            int size= list.size();
            for (int i=0; i<size-1; i++){
                dimensions="";
                String item= list.get(i);
                item = item.substring(1);
                System.out.println(item);
//              {"headers":{},"message":"{\"metric\":{\"dimensions\":{\"product\":\"vserver\",\"hostname\":\"server2\",\"zone\":\"HCM-02\",\"user_id\":11323,\"resource_id\":\"b7c6c707-03cd-416c-8949-73cf69723775\",\"device\":\"sda\"},\"name\":\"vserver.io.read_ops\",\"timestamp\":1657856138989,\"value\":26793.0,\"value_meta\":{}},\"meta\":{\"tenantId\":\"ab0df5851da44e22b97c44e977bb9cb8\",\"region\":\"RegionOne\"},\"creation_time\":1657856138989}","message_key":null,"offset":1518254536,"partition":0,"source_type":"kafka","timestamp":"2022-07-15T03:35:41.376Z",
                json= new JSONObject(item + "\"temp\":\"\"}");
                json= (new JSONObject(json.getString("message")));

                tenantId= json.getJSONObject("meta").getString("tenantId");
                // get matric in message
                json= json.getJSONObject("metric");

                //get dimensions in matric
                dimensionsJson= json.getJSONObject("dimensions");

                for (Iterator key=dimensionsJson.keys();key.hasNext();) {
                    keyvalue= key.next().toString();
                    dimensions = dimensions + keyvalue+ ":" + dimensionsJson.getString(keyvalue)+",";

                    //now name contains the firstname, and so on...
                }
                dimensions=dimensions.substring(0,dimensions.length()-1);
                System.out.println(dimensions);

                System.out.println(tenantId);
                System.out.println(json);
                arr.add(new Tuple5<String, Long, Double, String, String>(json.getString("name"),json.getLong("timestamp"),json.getDouble("value"),tenantId,dimensions));

            }
            DataSet<Tuple5<String, Long, Double, String, String>> data= env.fromCollection(arr);
            return data;
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
    }
    static List<String> readFile2(ExecutionEnvironment env){
        try {
            FileInputStream myObj = new FileInputStream("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/jar/input.log");
//            File myObj = new File("/home/lap12949/Documents/fresher/final-project/writemetric/src/input/input.txt");
            Scanner myReader = new Scanner(myObj);
            String data="";
            while (myReader.hasNextLine()) {
                data = myReader.nextLine();

            }
            List<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            Collector<String> myList = new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") ));
//            DataSet<List<String>> myList = env.fromElements(new ArrayList<String>(Arrays.asList(data.split("\"topic\":\"metrics\"}") )) );
            int size = myList.size();
//            myList.set(0, myList.get(0).substring(1));
            System.out.println(myList.get(0));
//            System.out.println(myList.get(size-1));
            myReader.close();
            return myList;

        } catch (FileNotFoundException e) {
            System.out.println("An error occurred.");
            e.printStackTrace();
        }
        return null;
    }
    public static class LineSplitter implements FlatMapFunction<Tuple5<String, Long, Double, String, JSONObject>, Tuple5<String, Long, Double, String, JSONObject>> {

        public void flatMap(Tuple5<String, Long, Double, String, JSONObject> line, Collector<Tuple5<String, Long, Double, String, JSONObject>> out) {
            System.out.println(1);
            try {
                out.collect(new Tuple5<>("",new Long(2),3.0,"",new JSONObject("{}")));
            } catch (JSONException e) {
                throw new RuntimeException(e);
            }
        }
    }
    public static void convertDayMetrics() throws Exception {

        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        Get variable form file
        String currentDir = System.getProperty("user.dir");
        InputStream is = new FileInputStream(currentDir+"/src/main/resources/log4j.properties");
        Properties p = new Properties();
        p.load(is);
        String region = p.getProperty("region");
        String access_key = p.getProperty("access_key");
        String secret_key = p.getProperty("secret_key");
        String host_base = p.getProperty("host_base");
        String startDay=p.getProperty("startDay");
        Date date= new SimpleDateFormat("yyyy-MM-dd").parse(startDay);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        String mode = p.getProperty("Mode");
        String hour = p.getProperty("startHour");
        long timestampForItem= Instant.parse( startDay+"T00:00:00Z" ).getEpochSecond()*1000;
//
//        Get file from S3
        long countmetric=0;
        List<GetObjectRequest> objectPortionList;
        DataSet<Tuple6<String, Long, Double, String, String, Integer>> result=null;
        String[] hourLists= {"00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"};
        DataSet<Tuple6<String, Long, Double, String, String, Integer>> metricsCustom ;
        DataSet<Tuple6<String, Long, Double, String, String, Integer>> temp ;
        for (int i=0; i<24; i++){
            objectPortionList= DownloadService.downloadFile(region,access_key,secret_key,host_base,cal,hourLists[i]);
            if (objectPortionList.size() == 0) continue;

            System.out.println(objectPortionList);
            List<String> metrics;

//        System.out.println(objectPortionList.get(0));

//      Read file and convert metrics to Dataset
            metrics= ReadMetric.readFile02(objectPortionList.get(0));
            metricsCustom=ReadMetric.convertToDataset(metrics, env, timestampForItem);

            int objSize= objectPortionList.size();
            countmetric +=metricsCustom.count();
            for (int j=1;j<objectPortionList.size();j++){
                System.out.println("Hour=="+hourLists[i]);
                System.out.println(j);
                metrics= ReadMetric.readFile02(objectPortionList.get(j));
                if (metrics.size()==0) continue;
                temp=ReadMetric.convertToDataset(metrics, env, timestampForItem);
                countmetric += temp.count();
                metricsCustom=metricsCustom.union(temp);
                if (j%10 == 0) metricsCustom=metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
            }
//        countmetric= metricsCustom.count();
//            metricsCustom= metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
            if (result==null) result=metricsCustom;
            else result.union(metricsCustom);
            result= result.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);

            System.out.println(countmetric);

        }
//        DataSet<Tuple6<String, Long, Double, String, String, Integer>> result=metricCustomList.get(0) ;
//        for (int i=1; i<metricCustomList.size();i++){
//            result= result.union(metricCustomList.get(i));
//            result= result.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
//        }
        long countmetricAfterMerge= 0;
        if (result!= null) countmetricAfterMerge=result.count();
        WriteToParquet.writeDatasetToFile02(result,mode,startDay);
        UploadService.UploadFile02(mode,startDay);
        System.out.print("Metric count = ");
        System.out.println(countmetric);
        System.out.print("Metric count after merge= ");
        System.out.println(countmetricAfterMerge);

    }
    public static void convertHourMetrics() throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        Get variable form file
        String currentDir = System.getProperty("user.dir");
        InputStream is = new FileInputStream(currentDir+"/src/main/resources/log4j.properties");
        Properties p = new Properties();
        p.load(is);
        String region = p.getProperty("region");
        String access_key = p.getProperty("access_key");
        String secret_key = p.getProperty("secret_key");
        String host_base = p.getProperty("host_base");
        String startDay=p.getProperty("startDay");
        Date date= new SimpleDateFormat("yyyy-MM-dd").parse(startDay);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        String mode = p.getProperty("Mode");
        String hour = p.getProperty("startHour");

        long timestampForItem= Instant.parse( startDay+"T"+hour+":00:00Z" ).getEpochSecond()*1000;
//        System.out.println(timestampForItem);
//        System.out.println(startDay+" "+hour+":00:00");
//        if (mode.equals("Hour")) return;


//        Get file from S3
        List<GetObjectRequest> objectPortionList= new ArrayList<>();

        objectPortionList=DownloadService.downloadFile(region,access_key,secret_key,host_base,cal,hour);


        if (objectPortionList.size() == 0){
            System.out.println("No data at "+hour+"h "+startDay);
            return;
        }
//        System.out.println(objectPortionList);
        List<String> metrics;
        DataSet<Tuple6<String, Long, Double, String, String, Integer>> metricsCustom ;
        DataSet<Tuple6<String, Long, Double, String, String, Integer>> temp ;
//        System.out.println(objectPortionList.get(0));

//      Read file and convert metrics to Dataset
        metrics= ReadMetric.readFile02(objectPortionList.get(0));
        metricsCustom=ReadMetric.convertToDataset(metrics, env, timestampForItem);

        int objSize= objectPortionList.size();
        long countmetric=metricsCustom.count();
        for (int i=1;i<objectPortionList.size();i++){
            System.out.println(i);
            metrics= ReadMetric.readFile02(objectPortionList.get(i));
            if (metrics.size()==0) continue;
            temp=ReadMetric.convertToDataset(metrics, env, timestampForItem);
            countmetric += temp.count();
            metricsCustom=metricsCustom.union(temp);
            if (i%10 == 0) metricsCustom=metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
//            if (i==100) break;
        }
        DataSet<Tuple6<String, Long, Double, String, String, Integer>> merge= metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
        long countmetricAfterMerge= merge.count();
        System.out.println("Metrics completed");
        WriteToParquet.writeDatasetToFile02(merge,mode,startDay);
        UploadService.UploadFile02(mode,startDay);
        System.out.print("Metric count = ");
        System.out.println(countmetric);
        System.out.print("Metric count after merge= ");
        System.out.println(countmetricAfterMerge);

    }
    public static void main(String[] args) throws Exception {
//        readFile();
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

//        Get variable form file
        String currentDir = System.getProperty("user.dir");
        InputStream is = new FileInputStream(currentDir+"/src/main/resources/log4j.properties");
        Properties p = new Properties();
        p.load(is);
        String region = p.getProperty("region");
        String access_key = p.getProperty("access_key");
        String secret_key = p.getProperty("secret_key");
        String host_base = p.getProperty("host_base");
        String startDay=p.getProperty("startDay");
        Date date= new SimpleDateFormat("yyyy-MM-dd").parse(startDay);
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        String mode = p.getProperty("Mode");
        String hour = p.getProperty("startHour");
        if (mode.equals("Day")){
            convertDayMetrics();
        } else {
            convertHourMetrics();
//            DownloadService.downloadFile("","","","",cal);

        }
////        Get file from S3
//        List<S3Object> objectPortionList= new ArrayList<>();
//        if (mode.equals("Hour")){
//            objectPortionList=DownloadService.hourPoint(region,access_key,secret_key,host_base,cal,hour);
//        }
//        else {
//            String[] hourLists= {"00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"};
//            for (int i=0; i<24; i++){
//                List<S3Object> objectPortionTemp= DownloadService.hourPoint(region,access_key,secret_key,host_base,cal,hourLists[i]);
//                if (objectPortionTemp.size() == 0) continue;
//                objectPortionList.addAll(objectPortionTemp);
//            }
////            objectPortionList=DownloadService.dayPoint(region,access_key,secret_key,host_base,cal,hour);
//        }
//        if (objectPortionList.size() == 0){
//            System.out.println("No data at "+hour+"h "+startDay);
//            return;
//        }
//        System.out.println(objectPortionList);
//        List<String> metrics;
//        DataSet<Tuple6<String, Long, Double, String, String, Integer>> metricsCustom ;
//        DataSet<Tuple6<String, Long, Double, String, String, Integer>> temp ;
////        System.out.println(objectPortionList.get(0));
//
////      Read file and convert metrics to Dataset
//        metrics= ReadMetric.readFile(objectPortionList.get(0));
//        metricsCustom=ReadMetric.convertToDataset(metrics, env);
//
//        int objSize= objectPortionList.size();
//        long countmetric=metricsCustom.count();
//        for (int i=1;i<objectPortionList.size();i++){
//            System.out.println(i);
//            metrics= ReadMetric.readFile(objectPortionList.get(i));
//            if (metrics.size()==0) continue;
//            temp=ReadMetric.convertToDataset(metrics, env);
//            countmetric += temp.count();
//            metricsCustom=metricsCustom.union(temp);
//            if (i%10 == 0) metricsCustom=metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
//        }
////        countmetric= metricsCustom.count();
//        DataSet<Tuple6<String, Long, Double, String, String, Integer>> merge= metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1).and(Aggregations.SUM,5);
//        long countmetricAfterMerge= merge.count();
////        merge= merge.sortPartition(0,Order.ASCENDING).sortPartition(3,Order.ASCENDING);
////        DataSet<Tuple2<String, String>> keyname= merge.project(0,3);
////        keyname=keyname.distinct(0,1);
////        long keynameCount= keyname.count();
////        WriteToParquet.writeDatset2(merge,keyname,mode,startDay);
//        WriteToParquet.writeDatasetToFile(merge,mode,startDay);
////        keyname.print();
//        System.out.println(countmetric);
//        System.out.println(countmetricAfterMerge);
////        System.out.println(keynameCount);
////        UploadService.UploadFile(mode,startDay);

    }
}


