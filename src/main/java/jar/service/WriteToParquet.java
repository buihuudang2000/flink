package jar.service;


import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.common.functions.*;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.util.Collector;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import scala.util.parsing.combinator.testing.Str;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.*;

public class WriteToParquet {

//    public static void readParquetFiles()  {
//
//        ParquetReader.Builder<Group> reader= ParquetReader
//                .builder(new GroupReadSupport(), new Path(filePath))
//                .withConf(new Configuration())
//                .withFilter(filter);
//
//        // read
//        ParquetReader<Group> build=reader.build();
//        Group line;
//        while((line=build.read())!=null)
//            System.out.println(line.toString())
//    }
    static List<GenericData.Record> readParquet(String outputPath){
        List<GenericData.Record> records=new ArrayList<>();
        File path = new File(outputPath);
        if (!path.exists()) return records;

        try (ParquetReader<GenericData.Record> reader = AvroParquetReader.<GenericData.Record>builder(new Path(outputPath))
                .withDataModel(new ReflectData(GenericData.Record.class.getClassLoader()))
                .disableCompatibility() // always use this (since this is a new project)
                .withConf(new Configuration())
                .build()) {

            GenericData.Record record;
            while ((record = reader.read()) != null) {
                records.add(record);
//                System.out.println("All records: " + record);
            }
            reader.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }

        return records;
    }
    public static void writeAvro(DataSet<Tuple2<Integer, String>> data, String outputPath) throws IOException, JSONException {



//        JSONObject json = new JSONObject("{\"Name\":\"dang\",\"Class\":1}");
////            JSONObject str= json.getJSONObject("metric");
//        System.out.println(json);

        String schemaString= "{\n" +
                "  \"type\": \"record\",\n" +
                "\"namespace\": \"avroschema\",\n" +
                "\"name\": \"Student\","+
                "\"fields\": [\n" +
                "{\"name\": \"Name\", \"type\": \"string\"},\n" +
                "{\"name\": \"Class\", \"type\": \"int\"}\n" +
                "]"+
                "}";
        final Schema schema = new Schema.Parser().parse(schemaString);
        System.out.println(schema);
        GenericData.Record record= new GenericData.Record(schema);
        record.put("Name","DangCharlie");
        record.put("Class",2);
        System.out.println(record);

        List<GenericData.Record> preData=  readParquet("/home/lap12949/Desktop/a.parquet");
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(new Path("/home/lap12949/Desktop/a.parquet"))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withSchema(schema)
                .build()) {
            for (GenericData.Record item: preData ){
                writer.write(item);
            }
            writer.write(record);
            record.put("Name","Dang");
            writer.write(record);

        }



    }
    static String createFolder(Tuple6<String, Long, Double, String, String, Integer> item, String mode, String startDay, String schemaString) throws JSONException, IOException {

        String[] temp= startDay.split("-");
        String dir = "/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/output/v1/"+temp[0]+"/"+ mode;
        File path = new File(dir);
        if (!path.exists()) {
            path.mkdir();
        }

        dir = dir +"/"+item.getField(3);
        path = new File(dir);
        if (!path.exists()) {
            path.mkdir();
        }

        dir = dir + "/"+item.getField(0);
        path = new File(dir);
        if (!path.exists()) {
            path.mkdir();
        }
        String namefile= dir+"/"+temp[1]+".parquet";
//        System.err.println(namefile);

        writePoint(item, namefile, schemaString);
        return namefile;
    }
    public static void writePoint(Tuple6<String, Long, Double, String, String, Integer> data, String outputPath, String schemaString) throws IOException, JSONException {

//        String schemaString= "{\n" +
//                "  \"type\": \"record\",\n" +
//                "\"namespace\": \"avroschema\",\n" +
//                "\"name\": \"Metrics\","+
//                "\"fields\": [\n" +
//                "{\"name\": \"Timestamp\", \"type\": \"long\"},\n" +
//                "{\"name\": \"Value\", \"type\": \"double\"},\n" +
//                "{\"name\": \"Dimensions\", \"type\": \"string\"}\n" +
//                "]"+
//                "}";
        final Schema schema = new Schema.Parser().parse(schemaString);
//        System.out.println(schema);

        GenericData.Record record= new GenericData.Record(schema);
        record.put("Timestamp",data.getField(1));
        double a= data.getField(2);
        int b= data.getField(5);
        double temp= a/b;
        record.put("Value",temp);
        record.put("Dimensions",data.getField(4));
        System.out.println(record);

        List<GenericData.Record> preData=  readParquet(outputPath);
        try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                .<GenericData.Record>builder(new Path(outputPath))
                .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                .withCompressionCodec(CompressionCodecName.SNAPPY)
                .withConf(new Configuration())
                .withSchema(schema)
                .build()) {
            for (GenericData.Record item: preData ){
                writer.write(item);
            }
            writer.write(record);

        }

    }
    public static void writeDatasetToFile(DataSet<Tuple6<String, Long, Double, String, String, Integer>> data, String mode, String startday) throws Exception {
        System.out.println("writeDatasetToFile");
        String schemaString= "{\n" +
                "  \"type\": \"record\",\n" +
                "\"namespace\": \"avroschema\",\n" +
                "\"name\": \"Metrics\","+
                "\"fields\": [\n" +
                "{\"name\": \"Timestamp\", \"type\": \"long\"},\n" +
                "{\"name\": \"Value\", \"type\": \"double\"},\n" +
                "{\"name\": \"Dimensions\", \"type\": \"string\"}\n" +
                "]"+
                "}";
//        Schema schema = new Schema.Parser().parse(schemaString);
//        System.out.println(schema);
        data.map(new MapFunction<Tuple6<String, Long, Double, String, String, Integer>, String>() {
            public String map(Tuple6<String, Long, Double, String, String, Integer> value) throws JSONException, IOException {
                String a= createFolder(value, mode, startday, schemaString);
                return a;
            }
        }).setParallelism(1);
    }



}
