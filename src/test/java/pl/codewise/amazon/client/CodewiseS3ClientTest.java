package pl.codewise.amazon.client;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import org.apache.commons.io.IOUtils;
import org.junit.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public class CodewiseS3ClientTest {

	public static final String ACCESS_KEY_PROPERTY_NAME = "s3.accessKey";
	public static final String SECRET_KEY_PROPERTY_NAME = "s3.secretKey";

	public static final String BUCKET_NAME_PROPERTY_NAME = "s3.bucketName";

	private String bucketName;
	private BasicAWSCredentials credentials;

	private TestS3Object PL = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/PL", 2);
	private TestS3Object US = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/05/US", 3);
	private TestS3Object CZ = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/06/CZ", 0);
	private TestS3Object UK = TestS3Object.withNameAndRandomMetadata("COUNTRY_BY_DATE/2014/07/UK", 1);

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

	@BeforeClass
	public void setUpS3Contents() {
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);

		PL.putToS3(amazonS3Client, bucketName);
		US.putToS3(amazonS3Client, bucketName);
		CZ.putToS3(amazonS3Client, bucketName);
		UK.putToS3(amazonS3Client, bucketName);
	}

	@AfterClass
	public void tearDown() {
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		PL.deleteFromS3(amazonS3Client, bucketName);
		US.deleteFromS3(amazonS3Client, bucketName);
		CZ.deleteFromS3(amazonS3Client, bucketName);
		UK.deleteFromS3(amazonS3Client, bucketName);
	}

	@Test
	public void shouldListObjectsInBucket() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		// When
		ObjectListing listing = client.listObjects(bucketName);
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName);

		// Then
		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing);
	}

	@Test
	public void shouldListObjects() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		// When
		ObjectListing listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");

		// Then
		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing);
	}

	@Test
	public void shouldListObjectsWhenUsingRequest() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/05/");

		// When
		ObjectListing listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		// Then
		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing);
	}

	@Test
	public void shouldListObjectBatches() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		// When & Then
		ObjectListing listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/05/");
		ObjectListing amazonListing = amazonS3Client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/05/");

		while (amazonListing.isTruncated()) {
			ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing);
			listing = client.listNextBatchOfObjects(listing);
			amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
		}

		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing).isNotTruncated();
	}

	@Test
	public void shouldListObjectBatchesWhenStartingWithARequest() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/06/");

		// When & Then
		ObjectListing listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		while (amazonListing.isTruncated()) {
			ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing);
			listing = client.listNextBatchOfObjects(listing);
			amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
		}

		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing).isNotTruncated();
	}

	@Test
	public void shouldListObjectWithMaxKeysLimit() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/");
		request.setMaxKeys(2);

		// When
		ObjectListing listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		// Then
		ObjectListingAssert.assertThat(listing)
				.isEqualTo(amazonListing)
				.isTruncated()
				.hasSize(2);
	}

	@Test
	public void shouldListObjectBatchesWhenUsingRequest() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setMaxKeys(2);
		request.setPrefix("COUNTRY_BY_DATE/2014/");

		// When & Then
		ObjectListing listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		while (amazonListing.isTruncated()) {
			ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing);
			listing = client.listNextBatchOfObjects(listing);
			amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);
		}

		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing).isNotTruncated();
	}

	@Test
	public void shouldReturnEmptyListingWhenNotTruncated() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/");

		ObjectListing listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);
		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing).isNotTruncated();

		// When
		listing = client.listNextBatchOfObjects(listing);
		amazonListing = amazonS3Client.listNextBatchOfObjects(amazonListing);

		// Then
		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing).isNotNull();
	}

	@Test
	public void shouldListCommonPrefixes() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ListObjectsRequest request = new ListObjectsRequest();
		request.setBucketName(bucketName);
		request.setPrefix("COUNTRY_BY_DATE/2014/05/");
		request.setDelimiter("/");

		// When
		ObjectListing listing = client.listObjects(request);
		ObjectListing amazonListing = amazonS3Client.listObjects(request);

		// Then
		ObjectListingAssert.assertThat(listing).isEqualTo(amazonListing).isNotTruncated();
	}

	@Test
	public void shouldGetObjects() throws IOException {
		// Given
		AmazonS3Client amazonS3Client = new AmazonS3Client(credentials);
		CodewiseS3Client client = new CodewiseS3Client(amazonS3Client, credentials);

		ObjectListing listing = client.listObjects(bucketName, "COUNTRY_BY_DATE/2014/");

		// When
		List<byte[]> actual = listing
				.getObjectSummaries()
				.stream()
				.map(object -> {
					try {
						return client.getObject(bucketName, object.getKey());
					} catch (Exception e) {
						throw new RuntimeException(e);
					}
				})
				.map(object -> {
					try {
						return IOUtils.toByteArray(object);
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				})
				.collect(Collectors.toList());

		List<byte[]> expected = listing
				.getObjectSummaries()
				.stream()
				.map(object -> amazonS3Client.getObject(bucketName, object.getKey()))
				.map(object -> {
					try {
						return IOUtils.toByteArray(object.getObjectContent());
					} catch (IOException e) {
						throw new UncheckedIOException(e);
					}
				})
				.collect(Collectors.toList());

		// Then
		assertThat(actual).hasSameSizeAs(expected);
		for (int i = 0; i < actual.size(); i++) {
			assertThat(actual.get(i)).isEqualTo(expected.get(i));
		}
	}
}
