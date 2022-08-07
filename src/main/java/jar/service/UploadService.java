package jar.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.PutObjectRequest;
import jar.config.AmazonS3ConfigToUpload;
import scala.Char;

import java.io.File;

import static jar.config.AmazonS3ConfigToUpload.s3client;

public class UploadService {
    public static void UploadFile(String mode, String startDay){

        AmazonS3 s3= AmazonS3ConfigToUpload.s3client();
        String fileUpload;
        String keyname;
        File file;
        System.out.println(AmazonS3ConfigToUpload.s3client().toString());
        String[] time= startDay.split("-");
        File[] directories = new File("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/output/v1/2022/Hour").listFiles(File::isDirectory);
        for (int i=0; i< directories.length; i++){
            File[] temp= directories[i].listFiles(File::isDirectory);
            for (int j=0; j< temp.length; j++){
                fileUpload= temp[j].toString()+"/"+time[1]+".parquet";
                System.out.println(fileUpload);
                file = new File(fileUpload);
                String[] folders3= temp[j].toString().split("/");
                keyname= time[0]+"/"+mode+"/"+folders3[folders3.length-2]+"/"+folders3[folders3.length-1]+"/"+time[1]+".parquet";
                System.out.println(keyname);
                s3.putObject(new PutObjectRequest("metrics-test-2807", keyname, file));

            }

        }

    }
    public static void UploadFile02(String mode, String startDay){

        AmazonS3 s3= AmazonS3ConfigToUpload.s3client();
        String dir="/home/lap12949/Documents/fresher/tjava/flink-1.13.6/flink-quickstart-java/src/main/java/output/v1/2022/"+mode;
        String fileUpload;
        String keyname;
        File file;
        System.out.println("Uploading File To AWS S3");
        String[] time= startDay.split("-");

        file = new File(dir +"/"+ time[2]+"-"+time[1]+".parquet");
        if (file.exists()){
            keyname= time[0]+"/"+mode+"/"+time[2]+"-"+time[1]+".parquet";
            s3.putObject(new PutObjectRequest("metrics-test-2807", keyname, file));
        }


    }
}
