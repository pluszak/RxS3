package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.ning.http.client.*;
import org.bouncycastle.crypto.digests.MD5Digest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.auth.AWSSignatureCalculatorAggregate;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;
import pl.codewise.amazon.client.xml.ListResponseParser;
import pl.codewise.amazon.client.xml.PassThroughParser;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static pl.codewise.amazon.client.RestUtils.createQueryString;
import static pl.codewise.amazon.client.RestUtils.escape;

public class CodewiseS3Client implements Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(CodewiseS3Client.class);

	private static final String S3_LOCATION = "s3.amazonaws.com";
	private static final String S3_URL = "http://" + S3_LOCATION;

	private final AmazonS3 client;
	private final AsyncHttpClient httpClient;

	private final ListResponseParser listResponseParser;
	private final ErrorResponseParser errorResponseParser;

	private final AWSSignatureCalculatorAggregate signatureCalculators;

	public CodewiseS3Client(AmazonS3 client, AWSCredentials credentials) {
		this.client = client;

		httpClient = new AsyncHttpClient();

		try {
			XmlPullParserFactory pullParserFactory = XmlPullParserFactory.newInstance();

			listResponseParser = new ListResponseParser(pullParserFactory);
			errorResponseParser = new ErrorResponseParser(pullParserFactory);
		} catch (XmlPullParserException e) {
			throw new RuntimeException("Unable to initialize xml pull parser factory", e);
		}

		signatureCalculators = new AWSSignatureCalculatorAggregate(credentials);
	}

	@SuppressWarnings("UnusedDeclaration")
	public void putObject(PutObjectRequest request) {
		client.putObject(request);
	}

	@SuppressWarnings("UnusedDeclaration")
	public InputStream getObject(String bucketName, String location) throws IOException {
		String virtualHost = getVirtualHost(bucketName);
		String url = S3_URL + "/" + escape(location);

		Request request = httpClient.prepareGet(url)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getGetSignatureCalculator())
				.build();

		return retrieveResult(request, new PassThroughParser());
	}

	@SuppressWarnings("UnusedDeclaration")
	public ListenableFuture<Response> deleteObject(String bucketName, String object) throws IOException {
		String virtualHost = getVirtualHost(bucketName);

		Request request = httpClient.prepareDelete(S3_URL + "/" + escape(object))
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getDeleteSignatureCalculator())
				.build();

		return httpClient.executeRequest(request, new AsyncCompletionHandler<Response>() {
			@Override
			public Response onCompleted(Response response) throws Exception {
				return response;
			}
		});
	}

	public ObjectListing listObjects(String bucketName) throws IOException {
		return listObjects(bucketName, null);
	}

	public ObjectListing listObjects(String bucketName, String prefix) throws IOException {
		String virtualHost = getVirtualHost(bucketName);
		String queryString = createQueryString(prefix, null, null, null);

		Request request = httpClient.prepareGet(S3_URL + "/?" + queryString)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getListSignatureCalculator())
				.build();

		return retrieveResult(request, listResponseParser);
	}

	public ObjectListing listNextBatchOfObjects(ObjectListing objectListing) throws IOException {
		if (!objectListing.isTruncated()) {
			ObjectListing emptyListing = new ObjectListing();
			emptyListing.setBucketName(objectListing.getBucketName());
			emptyListing.setDelimiter(objectListing.getDelimiter());
			emptyListing.setMarker(objectListing.getNextMarker());
			emptyListing.setMaxKeys(objectListing.getMaxKeys());
			emptyListing.setPrefix(objectListing.getPrefix());
			emptyListing.setEncodingType(objectListing.getEncodingType());
			emptyListing.setTruncated(false);

			return emptyListing;
		}

		return listObjects(new ListObjectsRequest(
				objectListing.getBucketName(),
				objectListing.getPrefix(),
				objectListing.getNextMarker(),
				objectListing.getDelimiter(),
				objectListing.getMaxKeys()
		));
	}

	public ObjectListing listObjects(ListObjectsRequest listObjectsRequest) throws IOException {
		String virtualHost = getVirtualHost(listObjectsRequest.getBucketName());
		String queryString = createQueryString(listObjectsRequest);

		Request request = httpClient.prepareGet(S3_URL + "/?" + queryString)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getListSignatureCalculator())
				.build();

		return retrieveResult(request, listResponseParser);
	}

	@Override
	public void close() throws IOException {
		httpClient.close();
	}

	@SuppressWarnings("UnusedDeclaration")
	public void deleteObjects(String bucketName, List<S3ObjectSummary> objectSummaries) throws IOException {
		StringBuilder builder = new StringBuilder();
		builder.append("<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n")
				.append("<Delete><Quiet>false</Quiet>\n");

		for (S3ObjectSummary objectSummary : objectSummaries) {
			builder.append("<Object><Key>")
					.append(escape(objectSummary.getKey()))
					.append("</Key></Object>");
		}
		builder.append("</Delete>");

		String string = builder.toString();
		byte[] bytes = string.getBytes();
		MD5Digest md5 = new MD5Digest();
		md5.update(bytes, 0, bytes.length);

		byte[] digest = new byte[md5.getDigestSize()];
		md5.doFinal(digest, 0);

		String virtualHost = getVirtualHost(bucketName);
		Request request = httpClient.preparePost(S3_URL + "/?delete")
				.setVirtualHost(virtualHost)
//				.addHeader("Content-MD5", Base64.getEncoder().encodeToString(digest))
				.addHeader("Content-Length", "" + bytes.length / 2)
				.setSignatureCalculator(signatureCalculators.getBulkDeleteSignatureCalculator())
				.setBody(bytes)
				.build();

//		retrieveResult(request);
	}

	private String getVirtualHost(String bucketName) {
		return bucketName + "." + S3_LOCATION;
	}

	private void throwExceptionIfUnsuccessful(Response response) throws IOException {
		if (response.getStatusCode() != 200) {
			throw errorResponseParser.parse(response).build();
		}
	}

	private <T> T retrieveResult(Request request, GenericResponseParser<T> responseParser) throws IOException {
		try {
			Response response = httpClient.executeRequest(request, new AsyncCompletionHandler<Response>() {
				@Override
				public Response onCompleted(Response response) throws Exception {
					return response;
				}
			}).get();

			if (LOGGER.isDebugEnabled()) {
				LOGGER.debug("Amazon response '{}'", response.getResponseBody());
			}
			throwExceptionIfUnsuccessful(response);

			return responseParser.parse(response);
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
	}
}
