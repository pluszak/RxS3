package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ObjectListing;
import io.reactivex.Single;
import org.assertj.core.api.Assertions;

public class AsyncS3ClientAssertions extends Assertions {

    public static ObjectListingAssert assertThat(ObjectListing objectListing) {
        return new ObjectListingAssert(objectListing);
    }

    public static ObjectListingAssert assertThat(Single<ObjectListing> objectListing) {
        return new ObjectListingAssert(objectListing.blockingGet());
    }
}
