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
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;
import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static pl.codewise.amazon.client.AsyncS3ClientAssertions.assertThat;

public class AsyncS3ClientTest {

	public static final String ACCESS_KEY_PROPERTY_NAME = "s3.accessKey";
	public static final String SECRET_KEY_PROPERTY_NAME = "s3.secretKey";

	public static final String BUCKET_NAME_PROPERTY_NAME = "s3.bucketName";

	private String bucketName;
	protected BasicAWSCredentials credentials;

	private TestS3Object PL = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/PL", 2);
	private TestS3Object US = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/US", 3);
	private TestS3Object CZ = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/06/CZ", 0);
	private TestS3Object UK = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/07/UK", 1);

	protected ClientConfiguration configuration;

	protected List<String> fieldsToIgnore = Lists.newArrayList();

	@BeforeClass
	public void setUpCredentialsAndBucketName() {
		String accessKey = System.getProperty(ACCESS_KEY_PROPERTY_NAME);
		assertThat(accessKey).isNotNull();

		String secretKey = System.getProperty(SECRET_KEY_PROPERTY_NAME);
		assertThat(secretKey).isNotNull();

		bucketName = System.getProperty(BUCKET_NAME_PROPERTY_NAME);
		assertThat(bucketName).isNotNull();
		credentials = new BasicAWSCredentials(accessKey, secretKey);
	}

	@BeforeClass(dependsOnMethods = "setUpCredentialsAndBucketName")
	public void setUpS3Contents() throws IOException {
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

		PL.putToS3(amazonS3Client, bucketName);
		US.putToS3(amazonS3Client, bucketName);
		CZ.putToS3(amazonS3Client, bucketName);
		UK.putToS3(amazonS3Client, bucketName);
	}

	@BeforeClass(dependsOnMethods = "setUpS3Contents")
	public void setUpConfigurationThatDoesNotSkipTags() throws IOException {
		configuration = ClientConfiguration
				.builder()
				.useCredentials(credentials)
				.build();
	}

	@AfterClass
	public void tearDown() {
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

		PL.deleteFromS3(amazonS3Client, bucketName);
		US.deleteFromS3(amazonS3Client, bucketName);
		CZ.deleteFromS3(amazonS3Client, bucketName);
		UK.deleteFromS3(amazonS3Client, bucketName);
	}

	@Test(enabled = false)
	public void shouldListObjectsInBucket() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

		// When
		Observable<ObjectListing> listing = client.listObjects(bucketName);
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName);

		// Then
		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing);
	}

	@Test(enabled = false)
	public void shouldListObjects() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

		// When
		Observable<ObjectListing> listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");

		// Then
		assertThat(listing)
				.ignoreFields(fieldsToIgnore)
				.isEqualTo(amazonListing);
	}

	@Test(enabled = false)
	public void shouldListObjectsWhenUsingRequest() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldListObjectBatches() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldListObjectBatchesWhenStartingWithARequest() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldListObjectWithMaxKeysLimit() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldListObjectBatchesWhenUsingRequest() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldReturnEmptyListingWhenNotTruncated() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldListCommonPrefixes() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldPutObject() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldGetObject() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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

	@Test(enabled = false)
	public void shouldDeleteObject() throws IOException, ExecutionException, InterruptedException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

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
		assertThat(CatchException.<Exception>caughtException()).hasMessageContaining("The specified key does not exist.");
	}

	private String getBase64EncodedMD5Hash(byte[] packet) {
		byte[] digest = DigestUtils.md5(packet);
		return new String(Base64.encodeBase64(digest));
	}
}
