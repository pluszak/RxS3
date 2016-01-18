package pl.codewise.amazon.client;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import javolution.text.TextBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xmlpull.v1.XmlPullParserException;
import org.xmlpull.v1.XmlPullParserFactory;
import pl.codewise.amazon.client.auth.AWSSignatureCalculatorFactory;
import pl.codewise.amazon.client.http.NettyHttpClient;
import pl.codewise.amazon.client.http.Request;
import pl.codewise.amazon.client.utils.TextBuilders;
import pl.codewise.amazon.client.utils.UTF8UrlEncoder;
import pl.codewise.amazon.client.xml.*;
import rx.Observable;
import rx.Subscriber;

import static pl.codewise.amazon.client.RestUtils.appendQueryString;

@SuppressWarnings("UnusedDeclaration")
public class AsyncS3Client implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncS3Client.class);

    private final NettyHttpClient httpClient;

    private final ListResponseParser listResponseParser;
    private final ErrorResponseParser errorResponseParser;

    private final AWSSignatureCalculatorFactory signatureCalculatorFactory;

    public AsyncS3Client(ClientConfiguration configuration, NettyHttpClient httpClient) {
        this.httpClient = httpClient;

        try {
            XmlPullParserFactory pullParserFactory = XmlPullParserFactory.newInstance();
            pullParserFactory.setNamespaceAware(false);

            listResponseParser = ListResponseParser.newListResponseParser(pullParserFactory, configuration);
            errorResponseParser = new ErrorResponseParser(pullParserFactory);
        } catch (XmlPullParserException e) {
            throw new RuntimeException("Unable to initialize xml pull parser factory", e);
        }

        signatureCalculatorFactory = new AWSSignatureCalculatorFactory(configuration.getCredentialsProvider());
    }

    public AsyncS3Client(ClientConfiguration configuration, HttpClientFactory httpClientFactory) {
        this(configuration, httpClientFactory.getHttpClient(configuration));
    }

    public int acquiredConnections() {
        return httpClient.acquiredConnections();
    }

    public Observable<InputStream> putObject(String bucketName, String key, byte[] data, ObjectMetadata metadata) throws IOException {
        Request request = httpClient.preparePut("/" + key)
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .setBody(data)
                .setContentLength((int) metadata.getContentLength())
                .setMd5(metadata.getContentMD5())
                .setContentType(metadata.getContentType())
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
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/?");
        appendQueryString(urlBuilder, prefix, null, null, null);

        Request request = httpClient.prepareList(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        retrieveResult(request, listResponseParser, subscriber);
    }

    public Observable<ObjectListing> listObjects(String bucketName, String prefix) {
        return Observable.create(subscriber -> listObjects(bucketName, prefix, subscriber));
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

    public void listObjects(ListObjectsRequest listObjectsRequest, Subscriber<? super ObjectListing> observer) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/?");
        appendQueryString(urlBuilder, listObjectsRequest);

        Request request = httpClient.prepareList(urlBuilder.toString())
                .setBucketName(listObjectsRequest.getBucketName())
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        retrieveResult(request, listResponseParser, observer);
    }

    public Observable<ObjectListing> listObjects(ListObjectsRequest listObjectsRequest) {
        return Observable.create(subscriber -> listObjects(listObjectsRequest, subscriber));
    }

    public Observable<InputStream> getObject(String bucketName, String location) throws IOException {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/");
        UTF8UrlEncoder.appendEncoded(urlBuilder, location);

        Request request = httpClient.prepareGet(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        return retrieveResult(request, ConsumeBytesParser.getInstance());
    }

    public Observable<?> deleteObject(String bucketName, String location) throws IOException {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/");
        UTF8UrlEncoder.appendEncoded(urlBuilder, location);

        Request request = httpClient.prepareDelete(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        return retrieveResult(request, DiscardBytesParser.getInstance());
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private <T> void retrieveResult(Request request, GenericResponseParser<T> responseParser, Subscriber<? super T> observer) {
        httpClient.executeRequest(request, new SubscriptionCompletionHandler<>(observer, responseParser, errorResponseParser));
    }

    private <T> Observable<T> retrieveResult(Request request, GenericResponseParser<T> responseParser) {
        return Observable.create(subscriber -> retrieveResult(request, responseParser, subscriber));
    }
}
