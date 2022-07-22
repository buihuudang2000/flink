package jar;


import java.io.*;
import java.util.*;

import jar.service.DownloadService;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.aggregation.Aggregations;
import org.apache.flink.api.java.operators.UnsortedGrouping;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.util.Collector;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple5;
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
//    public static List<Tuple5<String, Long, Double, String, JSONObject>> groupMetric(DataSet<Tuple5<String, Long, Double, String, JSONObject>> metrics){
//        metrics.map(System.out.println(1);)
//
//    }
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
//        System.out.println(DownloadService.downloadFile());
        List<String> metrics= readFile2(env);
        if (metrics==null) return;
        DataSet<Tuple5<String, Long, Double, String, String>> metricsCustom=convertToDataset(metrics, env);
        System.out.println("metricCustom");
//        DataSet<Tuple5<String, Long, Double, String, String>> temp= metricsCustom.union(metricsCustom);
        long countmetric1= metricsCustom.count();
        metricsCustom.print();
        DataSet<Tuple5<String, Long, Double, String, String>> check= metricsCustom.groupBy(0,3,4).aggregate(Aggregations.SUM,2).and(Aggregations.MIN,1);
        System.out.println("check");
        check.print();

        long countmetric2= check.count();
        System.out.println(countmetric2);

    }
}


