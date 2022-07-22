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
import java.util.List;

public class DownloadService{
    //    private static void displayTextInputStream(InputStream input) throws IOException
//    {
//        // Read one text line at a time and display.
//        BufferedReader reader = new BufferedReader(new InputStreamReader(input));
//        while(true)
//        {
//            String line = reader.readLine();
//            if(line == null) break;
//            System.out.println( "    " + line );
//        }
//        System.out.println();
//    }
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

    public static String downloadFile() throws IOException {

        AmazonS3 s3= AmazonS3Config.s3client();
//            BucketWebsiteConfiguration config = s3.getBucketWebsiteConfiguration("portal.vngcloud.vn");
//            S3Object s3object = s3.getObject("tunm4_metics.hcm01.vstorage.vngcloud.vn","dev/");
//            GetObjectRequest rangeObjectRequest = new GetObjectRequest("dev", "test=1/1658188870=e681aa71-ebcf-4d2c-b9e2-fa192bf40f5b.log");

        ObjectListing objects= s3.listObjects("dev", "date=2022-07-21/hour=07");
//            GetObjectRequest rangeObjectRequest = new GetObjectRequest("dev", "date=2022-07-21/hour=04/1658379847-fba24f81-0543-45f6-afce-f8607cea69be.log");
//            S3Object objectPortion = s3.getObject(rangeObjectRequest);
//
//            System.out.println("Printing bytes retrieved:");
//            displayTextInputStream(objectPortion.getObjectContent());

        String fileName = null;
        List<S3ObjectSummary> objectSummaries = objects.getObjectSummaries();

        for (S3ObjectSummary objectSummary : objectSummaries) {
            if (objectSummary.getKey().contains(".log")) {

//                fileName = URLEncoder.encode(objectSummary.getKey(), "UTF-8").replaceAll("\\+", "%20");
                fileName=objectSummary.getKey();
                GetObjectRequest rangeObjectRequest = new GetObjectRequest("dev", fileName);
                S3Object objectPortion = s3.getObject(rangeObjectRequest);

                System.out.println("Printing bytes retrieved:");
                displayTextInputStream(objectPortion.getObjectContent());
            }
        }

//        TransferManager tm = TransferManagerBuilder.standard()
//                .withS3Client(s3)
//                .build();
//
//        try {
//            MultipleFileDownload download = tm.downloadDirectory(
//                    "dev", "date=2022-07-20/hour=00", new File("/home/lap12949/Documents/fresher/tjava/flink-1.13.6/demo/output"));
//            download.waitForCompletion();
//            // loop with Transfer.isDone()
//
//            System.out.println("Download complete.");
//        } catch (InterruptedException e) {
//            throw new RuntimeException(e);
//        }
//        tm.shutdownNow();
        return s3.toString();
//            return "Hello";
    }


}