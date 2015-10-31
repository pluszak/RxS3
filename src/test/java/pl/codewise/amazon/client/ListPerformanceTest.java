package pl.codewise.amazon.client;

import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import rx.Observable;
import rx.Subscriber;
import rx.subjects.PublishSubject;

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
        client.listObjects(listing, new Subscriber<ObjectListing>() {
            @Override
            public void onCompleted() {
            }

            @Override
            public void onError(Throwable e) {
                subject.onError(e);
            }

            @Override
            public void onNext(ObjectListing objectListing) {
                subject.onNext(objectListing);
                if (objectListing.isTruncated()) {
                    client.listNextBatchOfObjects(objectListing, this);
                } else {
                    subject.onCompleted();
                }
            }
        });

        subject
                .flatMap(o -> Observable.from(o.getObjectSummaries()))
                .map(S3ObjectSummary::getKey)
                .subscribe(System.out::println, Throwable::printStackTrace, () -> System.out.println("Finished"));
    }
}
