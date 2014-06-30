package pl.codewise.amazon.client;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang3.RandomStringUtils;

import java.io.ByteArrayInputStream;

public class TestS3Object {

	private String objectName;

	private byte[] data;
	private ObjectMetadata metadata;

	private TestS3Object(String objectName, byte[] data, ObjectMetadata metadata) {
		this.objectName = objectName;

		this.data = data;
		this.metadata = metadata;
	}

	public void putToS3(AmazonS3Client client, String bucketName) {
		ByteArrayInputStream bais = new ByteArrayInputStream(data);
		client.putObject(bucketName, objectName, bais, metadata);
	}

	public void deleteFromS3(AmazonS3Client client, String bucketName) {
		client.deleteObject(bucketName, objectName);
	}

	public static TestS3Object withNameAndRandomMetadata(String objectName, int metadataEntries) {
		byte[] data = RandomStringUtils.random(42).getBytes();

		ObjectMetadata metadata = new ObjectMetadata();
		metadata.setContentLength(data.length);
		metadata.setContentType("application/octet-stream");
		metadata.setContentMD5(getBase64EncodedMD5Hash(data));

		for (int i = 0; i < metadataEntries; i++) {
			metadata.addUserMetadata(RandomStringUtils.randomAlphanumeric(5), RandomStringUtils.randomAlphanumeric(10));
		}

		return new TestS3Object(objectName, data, metadata);
	}

	private static String getBase64EncodedMD5Hash(byte[] packet) {
		byte[] digest = DigestUtils.md5(packet);
		return new String(Base64.encodeBase64(digest));
	}
}
