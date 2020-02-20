package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.AmazonS3Exception;
import io.reactivex.Single;
import io.reactivex.observers.TestObserver;
import io.reactivex.schedulers.TestScheduler;
import junit.framework.TestCase;
import org.apache.commons.lang3.mutable.MutableInt;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class GenericS3RetryTransformerTest extends TestCase {

    private static final int MAX_RETRIES = 4;

    private final TestScheduler scheduler = new TestScheduler();

    @Test
    public void shouldRetryIOExceptions() {
        // Given
        TestScheduler scheduler = new TestScheduler();

        Object successObject = new Object();
        Iterator<Object> subscribeActions = Arrays.asList(new IOException(), new IOException(), successObject).iterator();

        Single<Object> observable = Single.create(subscriber -> {
            Object action = subscribeActions.next();
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValue(successObject);
    }

    @Test
    public void shouldRetryUpToThreeIOExceptions() {
        // Given
        Single<Object> observable = Single.error(new IOException());

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertError(IOException.class);
    }

    @Test
    public void shouldRetryOnAmazonInternalError() {
        // Given
        Object successObject = new Object();
        Iterator<Object> subscribeActions = Arrays.asList(new AmazonS3Exception("InfernalError, Status Code: 500"), successObject).iterator();

        Single<Object> observable = Single.create(subscriber -> {
            Object action = subscribeActions.next();
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValue(successObject);
    }

    @Test
    public void shouldRetryUpToThreeAmazonInternalErrors() {
        // Given
        Single<Object> observable = Single.error(new AmazonS3Exception("InfernalError, Status Code: 500"));

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertError(AmazonS3Exception.class);
    }

    @Test
    public void shouldRetryOnTimeout() {
        // Given
        Object successObject = new Object();
        Iterator<Object> subscribeActions = Arrays.asList(new AmazonS3Exception("Status Code: 400; Error Code: RequestTimeout"), successObject).iterator();

        Single<Object> observable = Single.create(subscriber -> {
            Object action = subscribeActions.next();
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValue(successObject);
    }

    @Test
    public void shouldRetryOnRateLimit() {
        // Given
        Object successObject = new Object();
        Iterator<Object> subscribeActions = Arrays.asList(
                new RejectedExecutionException("operations under limit"),
                successObject
        ).iterator();

        Single<Object> observable = Single.create(subscriber -> {
            Object action = subscribeActions.next();
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoErrors();
        subscriber.assertValue(successObject);
    }

    @Test
    public void shouldNotRetryOtherAmazonErrors() {
        // Given
        Object successObject = new Object();
        Iterator<Object> subscribeActions = Arrays.asList(new AmazonS3Exception("Access Denied"), successObject).iterator();

        Single<Object> observable = Single.create(subscriber -> {
            Object action = subscribeActions.next();
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoValues();
        subscriber.assertError(AmazonS3Exception.class);
    }

    @Test
    public void shouldNotRetryOtherExceptions() {
        // Given
        Object successObject = new Object();
        Iterator<Object> subscribeActions = Arrays.asList(new ReflectiveOperationException(), successObject).iterator();

        Single<Object> observable = Single.create(subscriber -> {
            Object action = subscribeActions.next();
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        scheduler.advanceTimeBy(100, TimeUnit.SECONDS);
        subscriber.awaitTerminalEvent();
        subscriber.assertNoValues();
        subscriber.assertError(ReflectiveOperationException.class);
    }

    @Test
    public void shouldWaitBeforeRetrying() {
        // Given
        TestScheduler testScheduler = new TestScheduler();

        AmazonS3Exception lastError = new AmazonS3Exception("InfernalError, Status Code: 500");

        List<Object> expectedErrors = Arrays.asList(
                new AmazonS3Exception("Status Code: 400; Error Code: RequestTimeout"),
                new AmazonS3Exception("InfernalError, Status Code: 500"),
                new AmazonS3Exception("InfernalError, Status Code: 500"),
                lastError
        );

        MutableInt errorIndex = new MutableInt();
        Single<Object> observable = Single.create(subscriber -> {
            Object action = expectedErrors.get(errorIndex.getAndIncrement());
            if (action instanceof Exception) {
                subscriber.onError((Throwable) action);
            } else {
                subscriber.onSuccess(action);
            }
        });

        // When
        TestObserver<Object> subscriber = observable
                .compose(GenericS3RetryTransformer.addRetries(MAX_RETRIES, scheduler))
                .test();

        // Then
        subscriber.assertNoValues();
        subscriber.assertNoErrors();
        assertThat(errorIndex.intValue()).isEqualTo(1);

        testScheduler.advanceTimeBy(1, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        assertThat(errorIndex.intValue()).isEqualTo(2);

        testScheduler.advanceTimeBy(2, TimeUnit.SECONDS);
        subscriber.assertNoErrors();
        assertThat(errorIndex.intValue()).isEqualTo(3);

        testScheduler.advanceTimeBy(3, TimeUnit.SECONDS);
        assertThat(errorIndex.intValue()).isEqualTo(4);
        subscriber.assertError(lastError);
    }
}
