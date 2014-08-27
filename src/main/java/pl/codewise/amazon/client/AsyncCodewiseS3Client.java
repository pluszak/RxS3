package pl.codewise.amazon.client;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.ning.http.client.*;
import com.ning.http.client.providers.netty.NettyAsyncHttpProviderConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.auth.AWSSignatureCalculatorFactory;
import pl.codewise.amazon.client.xml.ConsumeBytesParser;
import pl.codewise.amazon.client.xml.ErrorResponseParser;
import pl.codewise.amazon.client.xml.GenericResponseParser;
import pl.codewise.amazon.client.xml.ListResponseParser;
import rx.Observer;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;

import static pl.codewise.amazon.client.RestUtils.createQueryString;
import static pl.codewise.amazon.client.RestUtils.escape;

public class AsyncCodewiseS3Client implements Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncCodewiseS3Client.class);

	public static final String S3_LOCATION = "s3.amazonaws.com";
	private static final String S3_URL = "http://" + S3_LOCATION;

	private final AsyncHttpClient httpClient;

	private final ListResponseParser listResponseParser;
	private final ErrorResponseParser errorResponseParser;

	private final AWSSignatureCalculatorFactory signatureCalculators;

	public AsyncCodewiseS3Client(AWSCredentials credentials) {
		NettyAsyncHttpProviderConfig providerConfig = new NettyAsyncHttpProviderConfig();
		providerConfig.addProperty(NettyAsyncHttpProviderConfig.REUSE_ADDRESS, true);

		AsyncHttpClientConfig config = new AsyncHttpClientConfig.Builder()
				.setAllowPoolingConnection(true)
				.setAsyncHttpClientProviderConfig(providerConfig)
				.setConnectionTimeoutInMs(1000)
				.setRequestTimeoutInMs(10000)
				.setFollowRedirects(false)
				.setMaximumConnectionsPerHost(1000)
				.setMaximumConnectionsTotal(1000)
				.setIOThreadMultiplier(1)
				.build();

		httpClient = new AsyncHttpClient(config);

		try {
			XmlPullParserFactory pullParserFactory = XmlPullParserFactory.newInstance();
			pullParserFactory.setNamespaceAware(false);

			listResponseParser = new ListResponseParser(pullParserFactory);
			errorResponseParser = new ErrorResponseParser(pullParserFactory);
		} catch (XmlPullParserException e) {
			throw new RuntimeException("Unable to initialize xml pull parser factory", e);
		}

		signatureCalculators = new AWSSignatureCalculatorFactory(credentials);
	}

	public void putObject(String bucketName, String key, InputStream inputStream, ObjectMetadata metadata, Observer<byte[]> observer) throws IOException {
		String virtualHost = getVirtualHost(bucketName);

		Request request = httpClient.preparePut(S3_URL + "/" + key)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getPutSignatureCalculator(bucketName))
				.setBody(inputStream)
				.setContentLength((int) metadata.getContentLength())
				.setHeader("Content-MD5", metadata.getContentMD5())
				.setHeader("Content-Type", metadata.getContentType())
				.build();

		retrieveResult(request, observer, ConsumeBytesParser.getInstance());
	}

	public void putObject(PutObjectRequest request, Observer<byte[]> observer) throws IOException {
		putObject(request.getBucketName(), request.getKey(), request.getInputStream(), request.getMetadata(), observer);
	}

	public void listObjects(String bucketName, Observer<ObjectListing> observer) {
		listObjects(bucketName, null, observer);
	}

	public void listObjects(String bucketName, String prefix, Observer<ObjectListing> observer) {
		String virtualHost = getVirtualHost(bucketName);
		String queryString = createQueryString(prefix, null, null, null);

		Request request = httpClient.prepareGet(S3_URL + "/?" + queryString)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getListSignatureCalculator(bucketName))
				.build();

		retrieveResult(request, observer, listResponseParser);
	}

	public void listNextBatchOfObjects(ObjectListing objectListing, Observer<ObjectListing> observer) {
		if (!objectListing.isTruncated()) {
			ObjectListing emptyListing = new ObjectListing();
			emptyListing.setBucketName(objectListing.getBucketName());
			emptyListing.setDelimiter(objectListing.getDelimiter());
			emptyListing.setMarker(objectListing.getNextMarker());
			emptyListing.setMaxKeys(objectListing.getMaxKeys());
			emptyListing.setPrefix(objectListing.getPrefix());
			emptyListing.setTruncated(false);

			observer.onNext(emptyListing);
		}

		listObjects(new ListObjectsRequest(
				objectListing.getBucketName(),
				objectListing.getPrefix(),
				objectListing.getNextMarker(),
				objectListing.getDelimiter(),
				objectListing.getMaxKeys()
		), observer);
	}

	public void listObjects(ListObjectsRequest listObjectsRequest, Observer<ObjectListing> observer) {
		String virtualHost = getVirtualHost(listObjectsRequest.getBucketName());
		String queryString = createQueryString(listObjectsRequest);

		Request request = httpClient.prepareGet(S3_URL + "/?" + queryString)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getListSignatureCalculator(listObjectsRequest.getBucketName()))
				.build();

		retrieveResult(request, observer, listResponseParser);
	}

	public void getObject(String bucketName, String location, Observer<byte[]> observer) throws IOException {
		String virtualHost = getVirtualHost(bucketName);
		String url = S3_URL + "/" + escape(location);

		Request request = httpClient.prepareGet(url)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.getGetSignatureCalculator(bucketName))
				.build();

		retrieveResult(request, observer, ConsumeBytesParser.getInstance());
	}

	@Override
	public void close() {
		httpClient.close();
	}

	private String getVirtualHost(String bucketName) {
		return bucketName + "." + S3_LOCATION;
	}

	private boolean emitExceptionIfUnsuccessful(Response response, Observer<?> observer) throws IOException {
		if (response.getStatusCode() != 200) {
			observer.onError(errorResponseParser.parse(response).get().build());
			return true;
		}

		return false;
	}

	private <T> void retrieveResult(final Request request, final Observer<T> observer, final GenericResponseParser<T> responseParser) {
		try {
			httpClient.executeRequest(request, new AsyncCompletionHandler<T>() {
				@Override
				public T onCompleted(Response response) throws IOException {
					if (LOGGER.isTraceEnabled()) {
						LOGGER.trace("Amazon response '{}'", response.getResponseBody());
					}

					if (!emitExceptionIfUnsuccessful(response, observer)) {
						try {
							Optional<T> result = responseParser.parse(response);
							result.ifPresent(observer::onNext);
							observer.onCompleted();
						} catch (Exception e) {
							observer.onError(e);
						}
					}

					return null;
				}
			});
		} catch (IOException e) {
			observer.onError(e);
		}
	}
}
