package pl.codewise.amazon.client;

import io.reactivex.Scheduler;
import io.reactivex.SingleTransformer;
import io.reactivex.schedulers.Schedulers;
import pl.codewise.amazon.client.http.NettyHttpClient;

public class S3ClientFactory {

    public static AsyncS3Client createClient(ClientConfiguration configuration) {
        NettyHttpClient httpClient = HttpClientFactory
                .defaultFactory()
                .getHttpClient(configuration);

        SingleTransformer transformer = createTransformerForRetryCount(
                configuration.getMaxRetries(),
                Schedulers.io()
        );

        return new AsyncS3Client(
                configuration,
                transformer,
                httpClient
        );
    }

    private static SingleTransformer createTransformerForRetryCount(
            int maxRetries,
            Scheduler timeoutScheduler
    ) {
        if (maxRetries > 0) {
            return GenericS3RetryTransformer.forRetries(
                    maxRetries,
                    timeoutScheduler
            );
        }

        return upstream -> upstream;
    }
}
