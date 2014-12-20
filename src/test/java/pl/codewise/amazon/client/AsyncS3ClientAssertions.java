package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ObjectListing;
import org.assertj.core.api.Assertions;
import rx.Observable;

public class AsyncS3ClientAssertions extends Assertions {

	public static ObjectListingAssert assertThat(ObjectListing objectListing) {
		return new ObjectListingAssert(objectListing);
	}

	public static ObjectListingAssert assertThat(Observable<ObjectListing> objectListing) {
		return new ObjectListingAssert(objectListing.toBlocking().single());
	}
}
