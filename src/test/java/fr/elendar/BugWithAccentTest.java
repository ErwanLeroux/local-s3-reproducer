package fr.elendar;

import com.robothy.s3.rest.LocalS3;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.ResponseBytes;
import software.amazon.awssdk.core.client.config.ClientOverrideConfiguration;
import software.amazon.awssdk.core.client.config.SdkAdvancedClientOption;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.core.sync.ResponseTransformer;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.S3Configuration;
import software.amazon.awssdk.services.s3.model.*;
import software.amazon.awssdk.utils.AttributeMap;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.assertj.core.api.Assertions.assertThat;
import static software.amazon.awssdk.http.SdkHttpConfigurationOption.TRUST_ALL_CERTIFICATES;

class BugWithAccentTest {
    private static final LocalS3 LOCAL_S3 = LocalS3.builder().port(-1).build();
    public static final String BUCKET_NAME = "bucket";
    private static S3Client s3Client;

    @BeforeAll
    static void beforeAll() {
        LOCAL_S3.start();
        s3Client = S3Client.builder()
                .region(Region.EU_WEST_3)
                .overrideConfiguration(ClientOverrideConfiguration.builder()
                        .putAdvancedOption(SdkAdvancedClientOption.DISABLE_HOST_PREFIX_INJECTION, true)
                        .build())
                .credentialsProvider(
                        StaticCredentialsProvider.create(AwsBasicCredentials.create("foo", "bar")))
                .serviceConfiguration(S3Configuration.builder().pathStyleAccessEnabled(true).build())
                .endpointOverride(URI.create("http://localhost:" + LOCAL_S3.getPort()))
                .httpClient(ApacheHttpClient.builder().buildWithDefaults(AttributeMap.builder()
                        .put(TRUST_ALL_CERTIFICATES, Boolean.TRUE)
                        .build()))
                // Uncomment to make bugWithContentArray pass
//                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
//                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .build();
    }

    @AfterAll
    static void afterAll() {
        LOCAL_S3.shutdown();
    }

    @BeforeEach
    void setUp() {
        if (!s3Client.listBuckets().buckets().stream().map(Bucket::name).toList().contains(BUCKET_NAME)) {
            s3Client.createBucket(CreateBucketRequest.builder().bucket(BUCKET_NAME).build());
        }
    }

    @Test
    void bugWithAccentTest() {
        String filename = "Tar_with_empty_gz.tar";
        String prefix = "folderWithAccenté/";
        String key = prefix + filename;
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(BUCKET_NAME).key(key).build();
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(Path.of("src/test/resources", filename)));

        // Throws an exception
        ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET_NAME).prefix(prefix).encodingType(EncodingType.URL).build());

        assertThat(listObjectsV2Response).isNotNull();
    }

    @Test
    void bugWithContentArray() throws IOException {
        String filename = "Tar_with_empty_gz.tar";
        String prefix = "folder/";
        String key = prefix + filename;
        PutObjectRequest putObjectRequest = PutObjectRequest.builder().bucket(BUCKET_NAME).key(key).build();
        Path pathToFile = Path.of("src/test/resources", filename);
        s3Client.putObject(putObjectRequest, RequestBody.fromFile(pathToFile));

        GetObjectRequest objectRequest = GetObjectRequest
                .builder()
                .bucket(BUCKET_NAME)
                .key(key)
                .build();

        ResponseBytes<GetObjectResponse> objectBytes = s3Client.getObject(objectRequest, ResponseTransformer.toBytes());
        byte[] data = objectBytes.asByteArray();

        // Should be the same array
        assertThat(data).isEqualTo(Files.readAllBytes(pathToFile));


        ListObjectsV2Response listObjectsV2Response = s3Client.listObjectsV2(ListObjectsV2Request.builder().bucket(BUCKET_NAME).prefix(prefix).encodingType(EncodingType.URL).build());

        assertThat(listObjectsV2Response).isNotNull();

    }
}
