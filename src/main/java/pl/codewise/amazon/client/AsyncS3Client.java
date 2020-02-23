package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.reactivex.*;
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

import static pl.codewise.amazon.client.RestUtils.appendQueryString;

/**
 * Note that CharSequence arguments will be copied to internal data structure before returning from AsyncS3Client method
 * call. This means that no CharSequence will be stored or passed to another thread and clients can safely reuse backing
 * storage (e.g. StringBuilder) after call to AsyncS3Client method returns.
 */
@SuppressWarnings("UnusedDeclaration")
public class AsyncS3Client implements AutoCloseable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AsyncS3Client.class);

    private final NettyHttpClient httpClient;
    @SuppressWarnings("rawtypes")
    private final SingleTransformer retryTransformer;

    private final ListResponseParser listResponseParser;
    private final ErrorResponseParser errorResponseParser;

    private final AWSSignatureCalculatorFactory signatureCalculatorFactory;

    public AsyncS3Client(
            ClientConfiguration configuration,
            SingleTransformer retryTransformer,
            NettyHttpClient httpClient) {
        this.retryTransformer = retryTransformer;
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

    public int acquiredConnections() {
        return httpClient.acquiredConnections();
    }

    public Completable putObject(String bucketName, CharSequence key, byte[] data, ObjectMetadata metadata) {
        return putObject(bucketName, key, Unpooled.wrappedBuffer(data), metadata)
                .ignoreElement();
    }

    public Single<?> putObject(String bucketName, CharSequence key, ByteBuf data, ObjectMetadata metadata) {
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

    public void listObjects(String bucketName, SingleEmitter<ObjectListing> subscriber) {
        listObjects(bucketName, null, subscriber);
    }

    public Single<ObjectListing> listObjects(String bucketName) {
        return listObjects(bucketName, (String) null);
    }

    private void listObjects(String bucketName, CharSequence prefix, SingleEmitter<? super ObjectListing> subscriber) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/?");
        appendQueryString(urlBuilder, prefix, null, null, null);

        Request request = httpClient.prepareList(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        retrieveResult(request, listResponseParser, subscriber);
    }

    public Single<ObjectListing> listObjects(String bucketName, CharSequence prefix) {
        return singleWithRetries(
                subscriber -> listObjects(
                        bucketName,
                        prefix,
                        subscriber
                )
        );
    }

    private void listNextBatchOfObjects(ObjectListing objectListing, SingleEmitter<ObjectListing> observable) {
        if (!objectListing.isTruncated()) {
            ObjectListing emptyListing = new ObjectListing();
            emptyListing.setBucketName(objectListing.getBucketName());
            emptyListing.setDelimiter(objectListing.getDelimiter());
            emptyListing.setMarker(objectListing.getNextMarker());
            emptyListing.setMaxKeys(objectListing.getMaxKeys());
            emptyListing.setPrefix(objectListing.getPrefix());
            emptyListing.setTruncated(false);

            observable.onSuccess(objectListing);
        }

        listObjects(new ListObjectsRequest(
                objectListing.getBucketName(),
                objectListing.getPrefix(),
                objectListing.getNextMarker(),
                objectListing.getDelimiter(),
                objectListing.getMaxKeys()), observable);
    }

    public Single<ObjectListing> listNextBatchOfObjects(ObjectListing objectListing) {
        if (!objectListing.isTruncated()) {
            ObjectListing emptyListing = new ObjectListing();
            emptyListing.setBucketName(objectListing.getBucketName());
            emptyListing.setDelimiter(objectListing.getDelimiter());
            emptyListing.setMarker(objectListing.getNextMarker());
            emptyListing.setMaxKeys(objectListing.getMaxKeys());
            emptyListing.setPrefix(objectListing.getPrefix());
            emptyListing.setTruncated(false);

            return Single.just(emptyListing);
        }

        return listObjects(new ListObjectsRequest(
                objectListing.getBucketName(),
                objectListing.getPrefix(),
                objectListing.getNextMarker(),
                objectListing.getDelimiter(),
                objectListing.getMaxKeys()));
    }

    private void listObjects(ListObjectsRequest listObjectsRequest, SingleEmitter<? super ObjectListing> observer) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/?");
        appendQueryString(urlBuilder, listObjectsRequest);

        Request request = httpClient.prepareList(urlBuilder.toString())
                .setBucketName(listObjectsRequest.getBucketName())
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        retrieveResult(request, listResponseParser, observer);
    }

    public Single<ObjectListing> listObjects(ListObjectsRequest listObjectsRequest) {
        return singleWithRetries(
                subscriber -> listObjects(
                        listObjectsRequest,
                        subscriber)
        );
    }

    public Single<GetObjectResponse> getObject(String bucketName, CharSequence location) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/");
        UTF8UrlEncoder.appendEncoded(urlBuilder, location);

        Request request = httpClient.prepareGet(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        return retrieveResult(request, ConsumeBytesParser.getInstance());
    }

    public Completable deleteObject(String bucketName, CharSequence location) {
        TextBuilder urlBuilder = TextBuilders.threadLocal();
        urlBuilder.append("/");
        UTF8UrlEncoder.appendEncoded(urlBuilder, location);

        Request request = httpClient.prepareDelete(urlBuilder.toString())
                .setBucketName(bucketName)
                .setSignatureCalculatorFactory(signatureCalculatorFactory)
                .build();

        return retrieveResult(request, DiscardBytesParser.getInstance())
                .ignoreElement();
    }

    @Override
    public void close() {
        httpClient.close();
    }

    private <T> void retrieveResult(Request request, GenericResponseParser<T> responseParser, SingleEmitter<? super T> observer) {
        SubscriptionCompletionHandler<T> completionHandler = new SubscriptionCompletionHandler<>(observer, request, responseParser, errorResponseParser);
        observer.setCancellable(completionHandler::cancel);

        httpClient.executeRequest(request, completionHandler);
    }

    private <T> Single<T> retrieveResult(Request request, GenericResponseParser<T> responseParser) {
        return singleWithRetries(emitter ->
                retrieveResult(
                        request,
                        responseParser,
                        emitter
                )
        );
    }

    @SuppressWarnings("unchecked")
    private <T> Single<T> singleWithRetries(SingleOnSubscribe<T> source) {
        return Single
                .create(source)
                .compose(retryTransformer);
    }
}
