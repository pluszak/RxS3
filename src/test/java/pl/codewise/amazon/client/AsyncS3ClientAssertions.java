package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ObjectListing;
import io.reactivex.Flowable;
import org.assertj.core.api.Assertions;

public class AsyncS3ClientAssertions extends Assertions {

	public static ObjectListingAssert assertThat(ObjectListing objectListing) {
		return new ObjectListingAssert(objectListing);
	}

	public static ObjectListingAssert assertThat(Flowable<ObjectListing> objectListing) {
		return new ObjectListingAssert(objectListing.blockingSingle());
	}
}
