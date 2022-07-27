package jar.service;

import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import jar.config.AmazonS3Config;
import com.amazonaws.services.s3.transfer.MultipleFileDownload;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;



import java.io.*;
import java.net.URLEncoder;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

public class DownloadService{

    private static void displayTextInputStream(InputStream input) throws IOException {
        // Read one text line at a time and display.
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
        while (true) {
            String line = reader.readLine();
            if (line == null)
                break;

            System.out.println("    " + line);
        }
        System.out.println();
    }

    public static String downloadFile(String region, String access_key, String secret_key, String host_base, Calendar cal) throws IOException {

        AmazonS3 s3= AmazonS3Config.s3client(region, access_key, secret_key, host_base);

        String prefix= "date=" + (new SimpleDateFormat("yyyy-MM-dd")).format(cal.getTime());
        List<S3ObjectSummary> objectSummaries = s3.listObjects("dev", prefix).getObjectSummaries();
        String fileName = null;
//        while (objectSummaries.size()!=0){
            for (S3ObjectSummary objectSummary : objectSummaries) {
                if (objectSummary.getKey().contains(".log")) {

    //                fileName = URLEncoder.encode(objectSummary.getKey(), "UTF-8").replaceAll("\\+", "%20");
                    fileName=objectSummary.getKey();

                    GetObjectRequest rangeObjectRequest = new GetObjectRequest("dev", fileName);
                    S3Object objectPortion = s3.getObject(rangeObjectRequest);

                    System.out.println("Printing bytes retrieved:");
                    displayTextInputStream(objectPortion.getObjectContent());
                    System.out.println(fileName);
                }
            }


        return s3.toString();

    }

    public static  List<S3Object>  hourPoint(String region, String access_key, String secret_key, String host_base, Calendar cal, String hour) throws IOException {

        AmazonS3 s3= AmazonS3Config.s3client(region, access_key, secret_key, host_base);
        List<S3Object> result= new ArrayList<>();
        String prefix= "date=" + (new SimpleDateFormat("yyyy-MM-dd")).format(cal.getTime())+"/hour="+hour;
        List<S3ObjectSummary> objectSummaries = s3.listObjects("dev", prefix).getObjectSummaries();
        String fileName = null;
        for (S3ObjectSummary objectSummary : objectSummaries) {
            if (objectSummary.getKey().contains(".log")) {

                //                fileName = URLEncoder.encode(objectSummary.getKey(), "UTF-8").replaceAll("\\+", "%20");
                fileName=objectSummary.getKey();

                GetObjectRequest rangeObjectRequest = new GetObjectRequest("dev", fileName);
                S3Object objectPortion = s3.getObject(rangeObjectRequest);
                result.add(objectPortion);
//                System.out.println("Printing bytes retrieved:");
//                displayTextInputStream(objectPortion.getObjectContent());
                System.out.println(fileName);
            }
        }


        return result;

    }

    public static  List<S3Object>  dayPoint(String region, String access_key, String secret_key, String host_base, Calendar cal, String hour) throws IOException {
        System.out.println("dayPonit");
        AmazonS3 s3= AmazonS3Config.s3client(region, access_key, secret_key, host_base);
        List<S3Object> result= new ArrayList<>();
        String prefix= "date=" + (new SimpleDateFormat("yyyy-MM-dd")).format(cal.getTime());
        List<S3ObjectSummary> objectSummaries = s3.listObjects("dev", prefix).getObjectSummaries();
        String fileName = null;
        for (S3ObjectSummary objectSummary : objectSummaries) {
            if (objectSummary.getKey().contains(".log")) {

                //                fileName = URLEncoder.encode(objectSummary.getKey(), "UTF-8").replaceAll("\\+", "%20");
                fileName=objectSummary.getKey();

                GetObjectRequest rangeObjectRequest = new GetObjectRequest("dev", fileName);
                S3Object objectPortion = s3.getObject(rangeObjectRequest);
                result.add(objectPortion);
//                System.out.println("Printing bytes retrieved:");
//                displayTextInputStream(objectPortion.getObjectContent());
                System.out.println(fileName);
            }
        }


        return result;

    }

}