package pl.codewise.amazon.client;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.google.common.collect.Lists;
import com.googlecode.catchexception.CatchException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.RandomStringUtils;
import org.joda.time.DateTimeZone;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import pl.codewise.amazon.fakes3.FakeS3;
import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.Locale;
import java.util.TimeZone;
import java.util.concurrent.ExecutionException;

import static org.slf4j.LoggerFactory.getLogger;
import static pl.codewise.amazon.client.AsyncS3ClientAssertions.assertThat;

public class AsyncS3ClientTest {

	private static final Logger LOGGER = getLogger(AsyncS3ClientTest.class);

	public static final String ACCESS_KEY_PROPERTY_NAME = "s3.accessKey";
	public static final String SECRET_KEY_PROPERTY_NAME = "s3.secretKey";
	public static final String EMPTY_CREDENTIAL = "empty";

	public static final String BUCKET_NAME_PROPERTY_NAME = "s3.bucketName";
	public static final String DEFAULT_BUCKET_NAME = "async-client-test";

	private static String bucketName;
	protected static BasicAWSCredentials credentials;

	private static TestS3Object PL = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/PL", 2);
	private static TestS3Object US = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/US", 3);
	private static TestS3Object CZ = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/06/CZ", 0);
	private static TestS3Object UK = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/07/UK", 1);

	protected static ClientConfiguration configuration;

	protected static List<String> fieldsToIgnore = Lists.newArrayList();

	private static FakeS3 fakeS3;
	private static AmazonS3Client amazonS3Client;
	private static AsyncS3Client client;

	@BeforeClass
	public static void setUpCredentialsAndBucketName() {
		TimeZone.setDefault(TimeZone.getTimeZone("UTC"));
		DateTimeZone.setDefault(DateTimeZone.UTC);
		Locale.setDefault(Locale.US);

		String accessKey = System.getProperty(ACCESS_KEY_PROPERTY_NAME, EMPTY_CREDENTIAL);
		String secretKey = System.getProperty(SECRET_KEY_PROPERTY_NAME, EMPTY_CREDENTIAL);

		credentials = new BasicAWSCredentials(accessKey, secretKey);
		amazonS3Client = new AmazonS3Client(credentials);

		if (EMPTY_CREDENTIAL.equals(accessKey) || EMPTY_CREDENTIAL.equals(secretKey)) {
			fakeS3 = new FakeS3();
			LOGGER.info("No amazon configuration was found. Using fake S3");

			amazonS3Client.setEndpoint("http://localhost:" + fakeS3.getLocalPort());
			configuration = ClientConfiguration
					.builder()
					.connectTo("localhost:" + fakeS3.getLocalPort())
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

	@BeforeClass
	public static void setUpS3Contents() throws IOException {
		PL.putToS3(amazonS3Client, bucketName);
		US.putToS3(amazonS3Client, bucketName);
		CZ.putToS3(amazonS3Client, bucketName);
		UK.putToS3(amazonS3Client, bucketName);
	}

	@AfterClass
	public static void tearDown() {
		PL.deleteFromS3(amazonS3Client, bucketName);
		US.deleteFromS3(amazonS3Client, bucketName);
		CZ.deleteFromS3(amazonS3Client, bucketName);
		UK.deleteFromS3(amazonS3Client, bucketName);

		IOUtils.closeQuietly(fakeS3);
	}

	@Test
	public void shouldListObjectsInBucket() throws IOException {
		// When
		Observable<ObjectListing> listing = client.listObjects(bucketName);
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName);

		// Then
		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing);
	}

	@Test
	public void shouldListObjects() throws IOException {
		// When
		Observable<ObjectListing> listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");

		// Then
		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing);
	}

	@Test
	public void shouldListObjectsWhenUsingRequest() throws IOException {
		// Given
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/05/");

		// When
		Observable<ObjectListing> listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		// Then
		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing);
	}

	@Test
	public void shouldListObjectBatches() throws IOException {
		// When & Then
		PublishSubject<ObjectListing> inProgressSubject = PublishSubject.create();
		PublishSubject<ObjectListing> completedSubject = PublishSubject.create();

		Observable<ObjectListing> listing = Observable.create((Subscriber<? super ObjectListing> subscriber) -> client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/05/", subscriber));
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/05/");

		inProgressSubject.subscribe(objectListing -> {
			completedSubject.onNext(objectListing);
			if (!objectListing.isTruncated()) {
				completedSubject.onCompleted();
			}

			client.listNextBatchOfObjects(objectListing).subscribe(inProgressSubject::onNext);
		});
		listing.subscribe(inProgressSubject::onNext);

		assertThat(completedSubject)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing).isNotTruncated();
	}

	@Test
	public void shouldListObjectBatchesWhenStartingWithARequest() throws IOException {
		// Given
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/06/");

		// When & Then
		Observable<ObjectListing> listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		while (amazonListing.isTruncated()) {
			assertThat(listing).isEqualTo(amazonListing);
			listing = client.listNextBatchOfObjects(listing.toBlocking().single());
			amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
		}

		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing).isNotTruncated();
	}

	@Test
	public void shouldListObjectWithMaxKeysLimit() throws IOException {
		// Given
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/");
		request.setMaxKeys(2);

		// When
		Observable<ObjectListing> listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		// Then
		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing)
				.isTruncated()
				.hasSize(2);
	}

	@Test
	public void shouldListObjectBatchesWhenUsingRequest() throws IOException {
		// Given
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setMaxKeys(2);
		request.setPrefix("COUNTRY_BY_DATE/2014/");

		// When & Then
		Observable<ObjectListing> listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		while (amazonListing.isTruncated()) {
			assertThat(listing)
					.ignoreFields(fieldsToIgnore)
					.isEqualTo(amazonListing);

			listing = client.listNextBatchOfObjects(listing.toBlocking().single());
			amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
		}

		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing)
				.isNotTruncated();
	}

	@Test
	public void shouldReturnEmptyListingWhenNotTruncated() throws IOException {
		// Given
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/");

		Observable<ObjectListing> listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing)
				.isNotTruncated();

		// When
		listing = client.listNextBatchOfObjects(listing.toBlocking().single());
		amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);

		// Then
		assertThat(listing).isEqualTo(amazonListing).isNotNull();
	}

	@Test
	public void shouldListCommonPrefixes() throws IOException {
		// Given
		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/05/");
		request.setDelimiter("/");

		// When
		Observable<ObjectListing> listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		// Then
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
		client.putObject(bucketName, objectName, data, metadata)
				.toBlocking()
				.single();

		// Then
		S3Object object = amazonS3Client.getObject(bucketName, objectName);
		byte[] actual = IOUtils.toByteArray(object.getObjectContent());

		assertThat(actual).isEqualTo(data);
	}

	@Test
	public void shouldGetObject() throws IOException {
		// Given
		String objectName = RandomStringUtils.randomAlphanumeric(55);
		byte[] data = RandomStringUtils.randomAlphanumeric(10 * 1024).getBytes();

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(data.length);
		metadata.setContentType("application/octet-stream");
		metadata.setContentMD5(getBase64EncodedMD5Hash(data));

		amazonS3Client.putObject(bucketName, objectName, new ByteArrayInputStream(data), metadata);

		// When
		byte[] actual = client.getObject(bucketName, objectName)
				.toBlocking()
				.single();

		// Then
		assertThat(actual).isEqualTo(data);
	}

	@Test
	public void shouldDeleteObject() throws IOException, ExecutionException, InterruptedException {
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
				.toBlocking()
				.singleOrDefault(null);

		// Then
		CatchException.catchException(amazonS3Client).getObject(bucketName, objectName);
		assertThat(CatchException.<Exception>caughtException()).hasMessageContaining("The specified key does not exist");
	}

	private String getBase64EncodedMD5Hash(byte[] packet) {
		byte[] digest = DigestUtils.md5(packet);
		return new String(Base64.encodeBase64(digest));
	}
}
