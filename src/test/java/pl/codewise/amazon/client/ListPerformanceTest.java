package pl.codewise.amazon.client;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import io.reactivex.FlowableEmitter;
import io.reactivex.Observable;
import io.reactivex.disposables.Disposable;
import io.reactivex.functions.Cancellable;
import io.reactivex.subjects.PublishSubject;


public class ListPerformanceTest {

    public static void main(String[] args) {
        BasicAWSCredentials credentials = new BasicAWSCredentials(args[0], args[1]);

        ClientConfiguration configuration = ClientConfiguration
                .builder()
                .useCredentials(credentials)
                .skipParsingOwner()
                .skipParsingETag()
                .skipParsingStorageClass()
                .build();

        AsyncS3Client client = new AsyncS3Client(configuration, HttpClientFactory.defaultFactory());

        PublishSubject<ObjectListing> subject = PublishSubject.create();

        ListObjectsRequest listing = new ListObjectsRequest()
                .withBucketName("voluum-prod-data")
                .withPrefix("+CLIENT_ID_CAMPAIGN_ID_TIME_HOUR_IP_NO_CONVERSIONS/2015/");

        client.listObjects(listing, new FlowableEmitter<ObjectListing>() {
            @Override
            public void setDisposable(Disposable s) {

            }

            @Override
            public void setCancellable(Cancellable c) {

            }

            @Override
            public long requested() {
                return 0;
            }

            @Override
            public boolean isCancelled() {
                return false;
            }

            @Override
            public FlowableEmitter<ObjectListing> serialize() {
                return null;
            }

            @Override
            public boolean tryOnError(Throwable t) {
                return false;
            }

            @Override
            public void onNext(ObjectListing objectListing) {
                subject.onNext(objectListing);
                if (objectListing.isTruncated()) {
                    client.listNextBatchOfObjects(objectListing, this);
                } else {
                    subject.onComplete();
                }
            }

            @Override
            public void onError(Throwable error) {

            }

            @Override
            public void onComplete() {

            }
        });

        subject
                .flatMap(o -> Observable.fromIterable(o.getObjectSummaries()))
                .map(S3ObjectSummary::getKey)
                .subscribe(System.out::println, Throwable::printStackTrace, () -> System.out.println("Finished"));
    }
}
