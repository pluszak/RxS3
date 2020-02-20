package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import io.reactivex.Flowable;
import io.reactivex.Scheduler;
import io.reactivex.SingleTransformer;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static io.reactivex.Flowable.range;

public class GenericS3RetryTransformer {

    private static final Logger LOGGER = LoggerFactory.getLogger(GenericS3RetryTransformer.class);

    public static SingleTransformer createTransformerForRetryCount(
            int maxRetries,
            Scheduler timeoutScheduler
    ) {
        if (maxRetries > 0) {
            return addRetries(maxRetries, timeoutScheduler);
        }

        return upstream -> upstream;
    }

    public static <T> SingleTransformer<T, T> addRetries(
            int maxRetries,
            Scheduler timeoutScheduler
    ) {
        return o -> o
                .retryWhen(observable ->
                        observable.zipWith(range(1, maxRetries), Pair::of)
                                .flatMap(pair -> {
                                    Throwable throwable = pair.getLeft();
                                    Integer retry = pair.getRight();

                                    if (retryLogic(
                                            maxRetries,
                                            retry,
                                            throwable
                                    )) {
                                        return Flowable.timer(
                                                retry,
                                                TimeUnit.SECONDS,
                                                timeoutScheduler
                                        );
                                    }

                                    return Flowable.<Integer>error(throwable);
                                })
                );
    }

    private static boolean retryLogic(
            int maxRetries,
            int retry,
            Throwable throwable
    ) {
        if (throwable instanceof RejectedExecutionException) {
            if (throwable.getMessage().contains("operations under limit")) {
                return true;
            }
        }

        if (retry < maxRetries) {
            if (throwable instanceof IOException) {
                LOGGER.debug("Retrying ({}) call that failed due to IOException: {}", retry, throwable.getMessage());
                return true;
            }

            if (throwable instanceof AmazonS3Exception) {
                boolean willRetry =
                        isInternalServerError(throwable) ||
                                isRequestTimeout(throwable) ||
                                isSlowDown(throwable);

                if (willRetry) {
                    LOGGER.debug("Retrying ({}) call that failed due to Amazon error: {}", retry, throwable.getMessage());
                }

                return willRetry;
            }

            if (throwable instanceof TimeoutException) {
                LOGGER.debug("Retrying ({}) call that timed out", retry);
                return true;
            }
        }

        LOGGER.debug("Not retrying ({}) call that failed due to {}", retry, throwable.getMessage());
        return false;
    }

    private static boolean isRequestTimeout(Throwable throwable) {
        return throwable.getMessage().contains("Status Code: 500");
    }

    private static boolean isInternalServerError(Throwable throwable) {
        return throwable.getMessage().contains("Error Code: RequestTimeout");
    }

    private static boolean isSlowDown(Throwable throwable) {
        return throwable.getMessage().contains("Slow Down");
    }
}
