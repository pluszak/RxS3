package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.ning.http.client.AsyncHttpClient;
import com.ning.http.client.Request;
import javolution.text.TextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.auth.AWSSignatureCalculatorFactory;
import pl.codewise.amazon.client.utils.UTF8UrlEncoder;
import pl.codewise.amazon.client.xml.*;
import rx.Observable;
import rx.Subscriber;

import java.io.Closeable;
import java.io.IOException;

import static pl.codewise.amazon.client.RestUtils.appendQueryString;

@SuppressWarnings("UnusedDeclaration")
public class AsyncS3Client implements Closeable {

	private static final Logger LOGGER = LoggerFactory.getLogger(AsyncS3Client.class);

	private final String s3Location;
	private final String s3Url;

	private final AsyncHttpClient httpClient;

	private final ListResponseParser listResponseParser;
	private final ErrorResponseParser errorResponseParser;

	private final ThreadLocal<AWSSignatureCalculatorFactory> signatureCalculators;

	public AsyncS3Client(ClientConfiguration configuration, AsyncHttpClient httpClient) {
		this.httpClient = httpClient;

		s3Location = configuration.getS3Location();
		s3Url = "http://" + s3Location;

		try {
			XmlPullParserFactory pullParserFactory = XmlPullParserFactory.newInstance();
			pullParserFactory.setNamespaceAware(false);

			listResponseParser = ListResponseParser.newListResponseParser(pullParserFactory, configuration);
			errorResponseParser = new ErrorResponseParser(pullParserFactory);
		} catch (XmlPullParserException e) {
			throw new RuntimeException("Unable to initialize xml pull parser factory", e);
		}

		signatureCalculators = new ThreadLocal<AWSSignatureCalculatorFactory>() {
			@Override
			protected AWSSignatureCalculatorFactory initialValue() {
				return new AWSSignatureCalculatorFactory(configuration.getCredentialsProvider(), s3Location);
			}
		};
	}

	public AsyncS3Client(ClientConfiguration configuration, HttpClientFactory httpClientFactory) {
		this(configuration, httpClientFactory.getHttpClient());
	}

	public Observable<byte[]> putObject(String bucketName, String key, byte[] data, ObjectMetadata metadata) throws IOException {
		String virtualHost = getVirtualHost(bucketName);

		Request request = httpClient.preparePut(s3Url + "/" + key)
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getPutSignatureCalculator())
				.setBody(data)
				.setContentLength((int) metadata.getContentLength())
				.setHeader("Content-MD5", metadata.getContentMD5())
				.setHeader("Content-Type", metadata.getContentType())
				.build();

		return retrieveResult(request, ConsumeBytesParser.getInstance());
	}

	public void listObjects(String bucketName, Subscriber<ObjectListing> subscriber) {
		listObjects(bucketName, null, subscriber);
	}

	public Observable<ObjectListing> listObjects(String bucketName) {
		return listObjects(bucketName, (String) null);
	}

	public void listObjects(String bucketName, String prefix, Subscriber<? super ObjectListing> subscriber) {
		TextBuilder urlBuilder = new TextBuilder();
		urlBuilder.append(s3Url).append("/?");
		appendQueryString(urlBuilder, prefix, null, null, null);

		String virtualHost = getVirtualHost(bucketName);
		Request request = httpClient.prepareGet(urlBuilder.toString())
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getListSignatureCalculator())
				.build();

		retrieveResult(request, listResponseParser, subscriber);
	}

	public Observable<ObjectListing> listObjects(String bucketName, String prefix) {
		TextBuilder urlBuilder = new TextBuilder();
		urlBuilder.append(s3Url).append("/?");
		appendQueryString(urlBuilder, prefix, null, null, null);

		String virtualHost = getVirtualHost(bucketName);
		Request request = httpClient.prepareGet(urlBuilder.toString())
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getListSignatureCalculator())
				.build();

		return retrieveResult(request, listResponseParser);
	}

	public void listNextBatchOfObjects(ObjectListing objectListing, Subscriber<ObjectListing> observable) {
		if (!objectListing.isTruncated()) {
			ObjectListing emptyListing = new ObjectListing();
			emptyListing.setBucketName(objectListing.getBucketName());
			emptyListing.setDelimiter(objectListing.getDelimiter());
			emptyListing.setMarker(objectListing.getNextMarker());
			emptyListing.setMaxKeys(objectListing.getMaxKeys());
			emptyListing.setPrefix(objectListing.getPrefix());
			emptyListing.setTruncated(false);

			observable.onNext(objectListing);
			observable.onCompleted();
		}

		listObjects(new ListObjectsRequest(
				objectListing.getBucketName(),
				objectListing.getPrefix(),
				objectListing.getNextMarker(),
				objectListing.getDelimiter(),
				objectListing.getMaxKeys()), observable);
	}

	public Observable<ObjectListing> listNextBatchOfObjects(ObjectListing objectListing) {
		if (!objectListing.isTruncated()) {
			ObjectListing emptyListing = new ObjectListing();
			emptyListing.setBucketName(objectListing.getBucketName());
			emptyListing.setDelimiter(objectListing.getDelimiter());
			emptyListing.setMarker(objectListing.getNextMarker());
			emptyListing.setMaxKeys(objectListing.getMaxKeys());
			emptyListing.setPrefix(objectListing.getPrefix());
			emptyListing.setTruncated(false);

			return Observable.just(emptyListing);
		}

		return listObjects(new ListObjectsRequest(
				objectListing.getBucketName(),
				objectListing.getPrefix(),
				objectListing.getNextMarker(),
				objectListing.getDelimiter(),
				objectListing.getMaxKeys()));
	}

	public void listObjects(ListObjectsRequest listObjectsRequest, Subscriber<ObjectListing> observer) {
		TextBuilder urlBuilder = new TextBuilder();
		urlBuilder.append(s3Url).append("/?");
		appendQueryString(urlBuilder, listObjectsRequest);

		String virtualHost = getVirtualHost(listObjectsRequest.getBucketName());
		Request request = httpClient.prepareGet(urlBuilder.toString())
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getListSignatureCalculator())
				.build();

		retrieveResult(request, listResponseParser, observer);
	}

	public Observable<ObjectListing> listObjects(ListObjectsRequest listObjectsRequest) {
		TextBuilder urlBuilder = new TextBuilder();
		urlBuilder.append(s3Url).append("/?");
		appendQueryString(urlBuilder, listObjectsRequest);

		String virtualHost = getVirtualHost(listObjectsRequest.getBucketName());
		Request request = httpClient.prepareGet(urlBuilder.toString())
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getListSignatureCalculator())
				.build();

		return retrieveResult(request, listResponseParser);
	}

	public Observable<byte[]> getObject(String bucketName, String location) throws IOException {
		TextBuilder urlBuilder = new TextBuilder();
		urlBuilder.append(s3Url).append("/");
		UTF8UrlEncoder.appendEncoded(urlBuilder, location);

		String virtualHost = getVirtualHost(bucketName);
		Request request = httpClient.prepareGet(urlBuilder.toString())
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getGetSignatureCalculator())
				.build();

		return retrieveResult(request, ConsumeBytesParser.getInstance());
	}

	public Observable<?> deleteObject(String bucketName, String location) throws IOException {
		TextBuilder urlBuilder = new TextBuilder();
		urlBuilder.append(s3Url).append("/");
		UTF8UrlEncoder.appendEncoded(urlBuilder, location);

		String virtualHost = getVirtualHost(bucketName);
		Request request = httpClient.prepareDelete(urlBuilder.toString())
				.setVirtualHost(virtualHost)
				.setSignatureCalculator(signatureCalculators.get().getDeleteSignatureCalculator())
				.build();

		return retrieveResult(request, DiscardBytesParser.getInstance());
	}

	@Override
	public void close() {
		httpClient.close();
	}

	private String getVirtualHost(String bucketName) {
		return bucketName + "." + s3Location;
	}

	private <T> void retrieveResult(final Request request, final GenericResponseParser<T> responseParser, Subscriber<? super T> observer) {
		httpClient.executeRequest(request, new SubscriptionCompletionHandler<>(observer, responseParser, errorResponseParser));
	}

	private <T> Observable<T> retrieveResult(final Request request, final GenericResponseParser<T> responseParser) {
		return Observable.create((Subscriber<? super T> subscriber) ->
						httpClient.executeRequest(request, new SubscriptionCompletionHandler<>(subscriber, responseParser, errorResponseParser))
		);
	}
}
