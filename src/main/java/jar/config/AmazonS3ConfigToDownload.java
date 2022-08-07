package jar.config;

import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AmazonS3ConfigToDownload {
    public static AmazonS3 s3client() {
        AWSCredentials l_credentials = new BasicAWSCredentials(
                "AKIA523ADVWY5IAJJJB7",
                "v6x09YvYrA1oICiTN4yyQaFvioySOY7anNH3zSMu"
        );
        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        return AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(l_credentials)).build();
        return AmazonS3ClientBuilder
                .standard()
                .withRegion(Regions.AP_SOUTHEAST_1)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(l_credentials))
                .build();
    }
}
