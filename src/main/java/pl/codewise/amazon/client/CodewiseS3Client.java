package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.ning.http.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.auth.AWSSignatureCalculatorFactory;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;
import pl.codewise.amazon.client.xml.ListResponseParser;
import pl.codewise.amazon.client.xml.PassThroughParser;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.concurrent.ExecutionException;

import static pl.codewise.amazon.client.RestUtils.createQueryString;
import static pl.codewise.amazon.client.RestUtils.escape;

public class CodewiseS3Client implements Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(CodewiseS3Client.class);

	private static final String S3_LOCATION = "s3.amazonaws.com";
	private static final String S3_URL = "http://" + S3_LOCATION;

	private final AsyncHttpClient httpClient;

	private final ListResponseParser listResponseParser;
	private final ErrorResponseParser errorResponseParser;

	private final AWSSignatureCalculatorFactory signatureCalculators;

	public CodewiseS3Client(AWSCredentials credentials, HttpClientFactory httpClientFactory) {
		this.httpClient = httpClientFactory.getHttpClient();

		try {
			XmlPullParserFactory pullParserFactory = XmlPullParserFactory.newInstance();

			listResponseParser = new ListResponseParser(pullParserFactory);
			errorResponseParser = new ErrorResponseParser(pullParserFactory);
		} catch (XmlPullParserException e) {
			throw new RuntimeException("Unable to initialize xml pull parser factory", e);
		}

		signatureCalculators = new AWSSignatureCalculatorFactory(credentials);
	}

	public void putObject(String bucketName, String key, InputStream inputStream, ObjectMetadata metadata) throws IOException {
		String virtualHost = getVirtualHost(bucketName);

		Request request = httpClient.preparePut(S3_URL + "/" + key)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getPutSignatureCalculator(bucketName))
				.setBody(inputStream)
				.setContentLength((int) metadata.getContentLength())
				.setHeader("Content-MD5", metadata.getContentMD5())
				.setHeader("Content-Type", metadata.getContentType())
				.build();

		retrieveResult(request, new PassThroughParser()).close();
	}

	public void putObject(PutObjectRequest request) throws IOException {
		putObject(request.getBucketName(), request.getKey(), request.getInputStream(), request.getMetadata());
	}

	public InputStream getObject(String bucketName, String location) throws IOException {
		String virtualHost = getVirtualHost(bucketName);
		String url = S3_URL + "/" + escape(location);

		Request request = httpClient.prepareGet(url)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getGetSignatureCalculator(bucketName))
				.build();

		return retrieveResult(request, new PassThroughParser());
	}

	@SuppressWarnings("UnusedDeclaration")
	public ListenableFuture<Response> deleteObject(String bucketName, String object) throws IOException {
		String virtualHost = getVirtualHost(bucketName);

		Request request = httpClient.prepareDelete(S3_URL + "/" + escape(object))
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getDeleteSignatureCalculator(bucketName))
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
				.setSignatureCalculator(signatureCalculators.getListSignatureCalculator(bucketName))
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
				.setSignatureCalculator(signatureCalculators.getListSignatureCalculator(listObjectsRequest.getBucketName()))
				.build();

		return retrieveResult(request, listResponseParser);
	}

	@Override
	public void close() {
		httpClient.close();
	}

	private String getVirtualHost(String bucketName) {
		return bucketName + "." + S3_LOCATION;
	}

	private void throwExceptionIfUnsuccessful(Response response) throws IOException {
		if (response.getStatusCode() != 200) {
			throw errorResponseParser.parseResponse(response).build();
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

			if (LOGGER.isTraceEnabled()) {
				LOGGER.trace("Amazon response '{}'", response.getResponseBody());
			}
			throwExceptionIfUnsuccessful(response);

			Optional<T> maybeResult = responseParser.parse(response);
			return maybeResult.orElse(null);
		} catch (InterruptedException | ExecutionException e) {
			throw new IOException(e);
		}
	}
}
