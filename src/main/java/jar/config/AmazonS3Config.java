package jar.config;


import com.amazonaws.ClientConfiguration;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

public class AmazonS3Config {
//    @Value("${spring.datasource.region}")
//    private static String region;
//
//    @Value("${spring.datasource.access_key}")
//    static String access_key;
//
//    @Value("${spring.datasource.secret_key}")
//    static String secret_key;

//    static  String region= "HCM01";
//    static String access_key="acb433b31e064b4bbe6994a87f0cb6ee";
//    static String secret_key="9205abdcb7216342bdbc127360af6435";

    public static AmazonS3 s3client(String region, String access_key,String secret_key,String host_base) {
        AWSCredentials l_credentials = new BasicAWSCredentials(
                access_key,
                secret_key
        );
        ClientConfiguration clientConfiguration = new ClientConfiguration();
//        return AmazonS3ClientBuilder.standard().withRegion(region).withCredentials(new AWSStaticCredentialsProvider(l_credentials)).build();
        return AmazonS3ClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(host_base, region))
                .withPathStyleAccessEnabled(true)
                .withClientConfiguration(clientConfiguration)
                .withCredentials(new AWSStaticCredentialsProvider(l_credentials))
                .build();
    }
}