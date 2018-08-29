package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivex.BackpressureStrategy;
import io.reactivex.Flowable;
import io.reactivex.FlowableEmitter;
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

import java.io.Closeable;

import static pl.codewise.amazon.client.RestUtils.appendQueryString;

/**
 * Note that CharSequence arguments will be copied to internal data structure before returning from AsyncS3Client method
 * call. This means that no CharSequence will be stored or passed to another thread and clients can safely reuse backing
 * storage (e.g. StringBuilder) after call to AsyncS3Client method returns.
 */
@SuppressWarnings("UnusedDeclaration")
public class AsyncS3Client implements Closeable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncS3Client.class);

    private final NettyHttpClient httpClient;

    private final ListResponseParser listResponseParser;
    private final ErrorResponseParser errorResponseParser;

    private final AWSSignatureCalculatorFactory signatureCalculatorFactory;

    private final BackpressureStrategy backpressureStrategy;

    public AsyncS3Client(ClientConfiguration configuration, NettyHttpClient httpClient) {
        this(configuration, httpClient, BackpressureStrategy.MISSING);
    }

    public AsyncS3Client(ClientConfiguration configuration, NettyHttpClient httpClient, BackpressureStrategy backpressureStrategy) {
        this.backpressureStrategy = backpressureStrategy;
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

    public Flowable<?> putObject(String bucketName, CharSequence key, byte[] data, ObjectMetadata metadata) {
        return putObject(bucketName, key, Unpooled.wrappedBuffer(data), metadata);
    }

    public Flowable<?> putObject(String bucketName, CharSequence key, ByteBuf data, ObjectMetadata metadata) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/")
                .append(key);

        Request request = httpClient.preparePut(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .setBody(data)
                .setContentLength((int) metadata.getContentLength())
                .setMd5(metadata.getContentMD5())
                .setContentType(metadata.getContentType())
                .build();

        return retrieveResult(request, DiscardBytesParser.getInstance());
    }

    public void listObjects(String bucketName, FlowableEmitter<ObjectListing> subscriber) {
        listObjects(bucketName, null, subscriber);
    }

    public Flowable<ObjectListing> listObjects(String bucketName) {
        return listObjects(bucketName, (String) null);
    }

    public void listObjects(String bucketName, CharSequence prefix, FlowableEmitter<? super ObjectListing> subscriber) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/?");
        appendQueryString(urlBuilder, prefix, null, null, null);

        Request request = httpClient.prepareList(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        retrieveResult(request, listResponseParser, subscriber);
    }

    public Flowable<ObjectListing> listObjects(String bucketName, CharSequence prefix) {
        return Flowable.create(subscriber -> listObjects(bucketName, prefix, subscriber), backpressureStrategy);
    }

    public void listNextBatchOfObjects(ObjectListing objectListing, FlowableEmitter<ObjectListing> observable) {
        if (!objectListing.isTruncated()) {
            ObjectListing emptyListing = new ObjectListing();
            emptyListing.setBucketName(objectListing.getBucketName());
            emptyListing.setDelimiter(objectListing.getDelimiter());
            emptyListing.setMarker(objectListing.getNextMarker());
            emptyListing.setMaxKeys(objectListing.getMaxKeys());
            emptyListing.setPrefix(objectListing.getPrefix());
            emptyListing.setTruncated(false);

            observable.onNext(objectListing);
            observable.onComplete();
        }

        listObjects(new ListObjectsRequest(
                objectListing.getBucketName(),
                objectListing.getPrefix(),
                objectListing.getNextMarker(),
                objectListing.getDelimiter(),
                objectListing.getMaxKeys()), observable);
    }

    public Flowable<ObjectListing> listNextBatchOfObjects(ObjectListing objectListing) {
        if (!objectListing.isTruncated()) {
            ObjectListing emptyListing = new ObjectListing();
            emptyListing.setBucketName(objectListing.getBucketName());
            emptyListing.setDelimiter(objectListing.getDelimiter());
            emptyListing.setMarker(objectListing.getNextMarker());
            emptyListing.setMaxKeys(objectListing.getMaxKeys());
            emptyListing.setPrefix(objectListing.getPrefix());
            emptyListing.setTruncated(false);

            return Flowable.just(emptyListing);
        }

        return listObjects(new ListObjectsRequest(
                objectListing.getBucketName(),
                objectListing.getPrefix(),
                objectListing.getNextMarker(),
                objectListing.getDelimiter(),
                objectListing.getMaxKeys()));
    }

    public void listObjects(ListObjectsRequest listObjectsRequest, FlowableEmitter<? super ObjectListing> observer) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/?");
        appendQueryString(urlBuilder, listObjectsRequest);

        Request request = httpClient.prepareList(urlBuilder.toString())
                .setBucketName(listObjectsRequest.getBucketName())
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        retrieveResult(request, listResponseParser, observer);
    }

    public Flowable<ObjectListing> listObjects(ListObjectsRequest listObjectsRequest) {
        return Flowable.create(subscriber -> listObjects(listObjectsRequest, subscriber), backpressureStrategy);
    }

    public Flowable<SizedInputStream> getObject(String bucketName, CharSequence location) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/");
        UTF8UrlEncoder.appendEncoded(urlBuilder, location);

        Request request = httpClient.prepareGet(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        return retrieveResult(request, ConsumeBytesParser.getInstance());
    }

    public Flowable<?> deleteObject(String bucketName, CharSequence location) {
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

    private <T> void retrieveResult(Request request, GenericResponseParser<T> responseParser, FlowableEmitter<? super T> observer) {
        SubscriptionCompletionHandler<T> completionHandler = new SubscriptionCompletionHandler<>(observer, request, responseParser, errorResponseParser);
        observer.setCancellable(() -> LOGGER.error("Cancelled request {}", request.getUrl()));

        httpClient.executeRequest(request, completionHandler);
    }

    private <T> Flowable<T> retrieveResult(Request request, GenericResponseParser<T> responseParser) {
        return Flowable.create(emitter -> retrieveResult(request, responseParser, emitter), backpressureStrategy);
    }
}
