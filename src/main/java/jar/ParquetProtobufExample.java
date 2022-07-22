package jar;

import com.sun.org.apache.bcel.internal.generic.Type;
import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.reflect.ReflectData;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.hadoop.mapred.HadoopOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.shaded.com.google.common.collect.ImmutableBiMap;
import org.apache.flink.types.IntValue;
import org.apache.flink.types.Record;
import org.apache.flink.types.StringValue;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.web.JsonUtil;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.avro.AvroParquetWriter;

import org.apache.parquet.hadoop.ParquetFileWriter;
import org.apache.parquet.hadoop.ParquetOutputFormat;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;
import org.codehaus.jackson.JsonNode;


import javax.security.auth.login.Configuration;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static junit.framework.Assert.assertEquals;
import static org.apache.avro.Schema.Type.LONG;
import static org.apache.avro.Schema.Type.NULL;

public class ParquetProtobufExample {

    public static void main(String[] args) throws Exception {


        //output
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSet<Tuple2<Integer, String>> output = putObjectIntoDataSet(env);
        writeAvro(output, "newpath");
        output.print();


    }

    public static DataSet<Tuple2<Integer, String>> putObjectIntoDataSet(ExecutionEnvironment env) {
        List<Tuple2<Integer, String>> arr=new ArrayList<>();
        arr.add(new Tuple2<Integer, String>(2,"hello"));
        DataSet<Tuple2<Integer, String>> data= env.fromCollection(arr);

        return data;
    }

//    public void testCreateUnionVarargs() {
//        List<Schema> types = new ArrayList<>();
//        types.add(Schema.create(NULL));
//        types.add(Schema.create(LONG));
//        Schema expected = Schema.createUnion(types);
//        Schema schema = Schema.createUnion(Schema.create(NULL), Schema.create(LONG));
//        assertEquals(expected, schema);
//    }

       public static void writeAvro(DataSet<Tuple2<Integer, String>> data, String outputPath) throws IOException, JSONException {



            JSONObject json = new JSONObject("{\"Name\":\"dang\",\"Class\":1}");
//            JSONObject str= json.getJSONObject("metric");
            System.out.println(json);

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
            record.put("Name","Dang");
            record.put("Class",2);
            System.out.println(record);
            try (ParquetWriter<GenericData.Record> writer = AvroParquetWriter
                    .<GenericData.Record>builder(new Path("/home/lap12949/Desktop/a.parquet"))
                    .withWriteMode(ParquetFileWriter.Mode.OVERWRITE)
                    .withCompressionCodec(CompressionCodecName.SNAPPY)
                    .withSchema(schema)
                    .build()) {writer.write(record);}
        // get timestamp from metric

        // convert int to datetime



    }


}

