package pl.codewise.amazon.client;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.*;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.http.Fault;
import com.github.tomakehurst.wiremock.stubbing.Scenario;
import com.googlecode.catchexception.CatchException;
import com.jayway.awaitility.Awaitility;
import com.jayway.awaitility.Duration;
import io.reactivex.Completable;
import io.reactivex.Single;
import io.reactivex.observers.TestObserver;
import io.reactivex.subjects.PublishSubject;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTimeZone;
import org.slf4j.Logger;
import org.testng.annotations.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;

import static com.github.tomakehurst.wiremock.client.WireMock.*;
import static org.slf4j.LoggerFactory.getLogger;
import static pl.codewise.amazon.client.AsyncS3ClientAssertions.assertThat;

public class AsyncS3ClientTest {

    private static final Logger LOGGER = getLogger(AsyncS3ClientTest.class);

    private static final String ACCESS_KEY_PROPERTY_NAME = "AWS_ACCESS_KEY";
    private static final String SECRET_KEY_PROPERTY_NAME = "AWS_SECRET_KEY";
    private static final String EMPTY_CREDENTIAL = "empty";

    private static final String BUCKET_NAME_PROPERTY_NAME = "s3.bucketName";
    private static final String DEFAULT_BUCKET_NAME = "async-client-test";

    private String bucketName;
    BasicAWSCredentials credentials;

    private TestS3Object PL = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/PL", 2);
    private TestS3Object US = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/US", 3);
    private TestS3Object CZ = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/06/CZ", 0);
    private TestS3Object UK = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/07/UK", 1);

    ClientConfiguration configuration;

    List<String> fieldsToIgnore = new ArrayList<>();

    private AmazonS3Client amazonS3Client;
    private AsyncS3Client client;

    WireMockServer wireMockServer = new WireMockServer();

    @BeforeClass
    public void setLocales() {
        TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
        DateTimeZone.setDefault(DateTimeZone.UTC);
        Locale.setDefault(Locale.US);

        wireMockServer.start();
    }

    @BeforeClass(dependsOnMethods = "setLocales")
    public void setUpCredentialsAndBucketName() {
        String accessKey = System.getenv(ACCESS_KEY_PROPERTY_NAME);
        if (accessKey == null) {
            accessKey = EMPTY_CREDENTIAL;
        }

        String secretKey = System.getenv(SECRET_KEY_PROPERTY_NAME);
        if (secretKey == null) {
            secretKey = EMPTY_CREDENTIAL;
        }

        credentials = new BasicAWSCredentials(accessKey, secretKey);
        amazonS3Client = new AmazonS3Client(credentials);

        if (EMPTY_CREDENTIAL.equals(accessKey) || EMPTY_CREDENTIAL.equals(secretKey)) {
            LOGGER.info("No amazon configuration was found. Assuming fake S3 listens on port 12345");

            amazonS3Client.setEndpoint("http://localhost:12345");
            configuration = ClientConfiguration
                    .builder()
                    .connectTo("localhost:12345")
                    .useCredentials(credentials)
                    .build();
        } else {
            configuration = ClientConfiguration
                    .builder()
                    .useCredentials(credentials)
                    .build();

            LOGGER.info("Found amazon configuration. Using real S3");
        }

        client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());
        bucketName = System.getProperty(BUCKET_NAME_PROPERTY_NAME, DEFAULT_BUCKET_NAME);
    }

    @BeforeClass(dependsOnMethods = "setUpCredentialsAndBucketName")
    public void setUpS3Contents() throws IOException {
        PL.putToS3(amazonS3Client, bucketName);
        US.putToS3(amazonS3Client, bucketName);
        CZ.putToS3(amazonS3Client, bucketName);
        UK.putToS3(amazonS3Client, bucketName);
    }

    @AfterClass
    public void tearDown() {
        PL.deleteFromS3(amazonS3Client, bucketName);
        US.deleteFromS3(amazonS3Client, bucketName);
        CZ.deleteFromS3(amazonS3Client, bucketName);
        UK.deleteFromS3(amazonS3Client, bucketName);

        wireMockServer.stop();
    }

    @BeforeMethod
    public void beforeTest() {
        Awaitility.await().atMost(Duration.TEN_SECONDS).until(() -> {
                    System.out.println("Acquired connections " + client.acquiredConnections());
                    assertThat(client.acquiredConnections()).isEqualTo(0);
                }
        );
    }

    @AfterMethod
    public void afterTest() {
        Awaitility.await().atMost(Duration.TEN_SECONDS).until(() -> {
                    System.out.println("Acquired connections " + client.acquiredConnections());
                    assertThat(client.acquiredConnections()).isEqualTo(0);
                }
        );
    }

    @Test
    public void shouldListObjectsInBucket() {
        // When
        Single<ObjectListing> listing = client.listObjects(bucketName);
        ObjectListing amazonListing = amazonS3Client.listObjects(bucketName);

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing);
    }

    @Test
    public void shouldListObjects() {
        // When
        Single<ObjectListing> listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");
        ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing);
    }

    @Test
    public void shouldListObjectsWhenUsingRequest() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/05/");

        // When
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing);
    }

    @Test
    public void shouldListObjectBatches() {
        // When & Then
        PublishSubject<ObjectListing> inProgressSubject = PublishSubject.create();
        PublishSubject<ObjectListing> completedSubject = PublishSubject.create();

        Single<ObjectListing> listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/05/");
        ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/05/");

        inProgressSubject.subscribe(objectListing -> {
            completedSubject.onNext(objectListing);
            if (!objectListing.isTruncated()) {
                completedSubject.onComplete();
            } else {
                client.listNextBatchOfObjects(objectListing).subscribe(inProgressSubject::onNext);
            }
        }, completedSubject::onError, completedSubject::onComplete);

        listing.subscribe(t -> {
            inProgressSubject.onNext(t);
            inProgressSubject.onComplete();
        }, inProgressSubject::onError);

        assertThat(completedSubject.blockingFirst())
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();
    }

    @Test
    public void shouldListObjectBatchesWhenStartingWithARequest() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/06/");

        // When & Then
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        while (amazonListing.isTruncated()) {
            assertThat(listing).isEqualTo(amazonListing);
            listing = client.listNextBatchOfObjects(listing.blockingGet());
            amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
        }

        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing).isNotTruncated();
    }

    @Test
    public void shouldListObjectWithMaxKeysLimit() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/");
        request.setMaxKeys(2);

        // When
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isTruncated()
                .hasSize(2);
    }

    @Test
    public void shouldListObjectBatchesWhenUsingRequest() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setMaxKeys(2);
        request.setPrefix("COUNTRY_BY_DATE/2014/");

        // When & Then
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        while (amazonListing.isTruncated()) {
            assertThat(listing)
                    .ignoreFields(fieldsToIgnore)
                    .isEqualTo(amazonListing);

            listing = client.listNextBatchOfObjects(listing.blockingGet());
            amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
        }

        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();
    }

    @Test
    public void shouldReturnEmptyListingWhenNotTruncated() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/");

        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();

        // When
        listing = client.listNextBatchOfObjects(listing.blockingGet());
        amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);

        // Then
        assertThat(listing).isEqualTo(amazonListing).isNotNull();
    }

    @Test
    public void shouldListCommonPrefixes_ContainingFiles() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/05/");
        request.setDelimiter("/");

        // When
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();
    }

    @Test
    public void shouldListCommonPrefixesInBatches_ContainingFiles() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/05/");
        request.setMaxKeys(1);
        request.setDelimiter("/");

        // When
        ObjectListing amazonListing = amazonS3Client.listObjects(request);
        Single<ObjectListing> listing = client.listObjects(request);

        // Then
        while (amazonListing.isTruncated()) {
            ObjectListing objectListing = listing.blockingGet();

            assertThat(objectListing)
                    .ignoreFields(fieldsToIgnore)
                    .isEqualTo(amazonListing)
                    .isTruncated();

            amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
            listing = client.listNextBatchOfObjects(objectListing);
        }

        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();
    }

    @Test
    public void shouldListCommonPrefixes_ContainingDirectories() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/");
        request.setDelimiter("/");

        // When
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();
    }

    @Test
    public void shouldListCommonPrefixesInBatches_ContainingDirectories() {
        // Given
        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/");
        request.setMaxKeys(1);
        request.setDelimiter("/");

        // When
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        // Then
        while (amazonListing.isTruncated()) {
            ObjectListing objectListing = listing.blockingGet();

            assertThat(objectListing)
                    .ignoreFields(fieldsToIgnore)
                    .isEqualTo(amazonListing)
                    .isTruncated();

            amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
            listing = client.listNextBatchOfObjects(objectListing);
        }

        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing)
                .isNotTruncated();
    }

    @Test
    public void shouldPutObject() throws IOException {
        // Given
        String objectName = RandomStringUtils.randomAlphanumeric(55);
        byte[] data = RandomStringUtils.randomAlphanumeric(10 * 1024).getBytes();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        metadata.setContentType("application/octet-stream");
        metadata.setContentMD5(getBase64EncodedMD5Hash(data));

        // When
        Completable completable = client.putObject(bucketName, objectName, data, metadata);

        // Then
        TestObserver<?> subscriber1 = completable.test();
        subscriber1.awaitTerminalEvent();
        subscriber1.assertNoErrors();
        subscriber1.assertComplete();

        TestObserver<?> subscriber2 = completable.test();

        subscriber2.awaitTerminalEvent();
        subscriber2.assertNoErrors();
        subscriber2.assertComplete();

        TestObserver<?> subscriber3 = completable.test();

        subscriber3.awaitTerminalEvent();
        subscriber3.assertNoErrors();
        subscriber3.assertComplete();

        S3Object object = amazonS3Client.getObject(bucketName, objectName);
        byte[] actual = IOUtils.toByteArray(object.getObjectContent());

        assertThat(actual).isEqualTo(data);
    }

    @Test
    public void shouldGetObject() {
        // Given
        String objectName = RandomStringUtils.randomAlphanumeric(55);
        byte[] data = RandomStringUtils.randomAlphanumeric(10 * 1024).getBytes();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        metadata.setContentType("application/octet-stream");
        metadata.setContentMD5(getBase64EncodedMD5Hash(data));

        amazonS3Client.putObject(bucketName, objectName, new ByteArrayInputStream(data), metadata);

        // When
        InputStream actual = client.getObject(bucketName, objectName)
                .blockingGet()
                .getContent();

        // Then
        assertThat(actual).hasContentEqualTo(new ByteArrayInputStream(data));
    }

    @Test
    public void shouldReturnObjectMetadataInGetObject() {
        // Given
        String objectName = RandomStringUtils.randomAlphanumeric(55);
        byte[] data = RandomStringUtils.randomAlphanumeric(10 * 1024).getBytes();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        metadata.setContentType("application/octet-stream");
        metadata.setContentMD5(getBase64EncodedMD5Hash(data));

        PutObjectResult putObjectResult = amazonS3Client
                .putObject(
                        bucketName,
                        objectName,
                        new ByteArrayInputStream(data),
                        metadata
                );

        // When
        String actual = client.getObject(bucketName, objectName)
                .blockingGet()
                .getETag();

        // Then
        assertThat(actual)
                .isEqualTo("\"" + putObjectResult.getETag() + "\"");
    }

    @Test
    public void shouldDeleteObject() {
        // Given
        String objectName = RandomStringUtils.randomAlphanumeric(55);
        byte[] data = RandomStringUtils.randomAlphanumeric(10 * 1024).getBytes();

        ObjectMetadata metadata = new ObjectMetadata();
        metadata.setContentLength(data.length);
        metadata.setContentType("application/octet-stream");
        metadata.setContentMD5(getBase64EncodedMD5Hash(data));

        amazonS3Client.putObject(bucketName, objectName, new ByteArrayInputStream(data), metadata);

        // When
        client.deleteObject(bucketName, objectName)
                .blockingAwait();

        // Then
        CatchException.catchException(amazonS3Client).getObject(bucketName, objectName);
        assertThat(CatchException.<Exception>caughtException()).hasMessageContaining("The specified key does not exist");
    }

    @Test
    public void shouldTimeoutAfterOneSecond_Read() {
        // Given
        wireMockServer.resetAll();
        wireMockServer.stubFor(any(anyUrl())
                .willReturn(
                        aResponse()
                                .withFixedDelay(10000)
                                .withStatus(200)
                )
        );

        ClientConfiguration configuration = ClientConfiguration
                .builder()
                .connectTo("locals3:" + wireMockServer.port())
                .timeoutRequestsAfter(1)
                .useCredentials(credentials)
                .build();

        AsyncS3Client client = new AsyncS3Client(
                configuration,
                HttpClientFactory.defaultFactory()
        );

        // When
        TestObserver<GetObjectResponse> testObserver = client
                .getObject("test", "foobar")
                .test();

        // Then
        testObserver.awaitTerminalEvent();
        testObserver.assertError(IOException.class);
        testObserver.assertErrorMessage("Channel become inactive");
    }

    @Test(enabled = false)
    public void shouldRetryListingObjectsWhenUsingRequest() {
        // Given
        wireMockServer.resetAll();
        wireMockServer.stubFor(any(anyUrl())
                .inScenario("retries")
                .whenScenarioStateIs(Scenario.STARTED)
                .willReturn(
                        aResponse()
                                .withFault(Fault.CONNECTION_RESET_BY_PEER)
                )
                .willSetStateTo("after-timeout")
        );

        wireMockServer.stubFor(any(anyUrl())
                .inScenario("retries")
                .whenScenarioStateIs("after-timeout")
                .willReturn(
                        aResponse()
                                .proxiedFrom("http://s3.amazonaws.com")
                )
        );

        ClientConfiguration configuration = ClientConfiguration
                .builder()
                .connectTo("locals3:" + wireMockServer.port())
                .withRetriesEnabled(1)
                .useCredentials(credentials)
                .build();

        AsyncS3Client client = new AsyncS3Client(
                configuration,
                HttpClientFactory.defaultFactory()
        );

        ListObjectsRequest request = new ListObjectsRequest();
        request.setBucketName(bucketName);
        request.setPrefix("COUNTRY_BY_DATE/2014/05/");

        // When
        Single<ObjectListing> listing = client.listObjects(request);
        ObjectListing amazonListing = amazonS3Client.listObjects(request);

        // Then
        assertThat(listing)
                .ignoreFields(fieldsToIgnore)
                .isEqualTo(amazonListing);
    }

    private String getBase64EncodedMD5Hash(byte[] packet) {
        byte[] digest = DigestUtils.md5(packet);
        return new String(Base64.encodeBase64(digest));
    }
}
