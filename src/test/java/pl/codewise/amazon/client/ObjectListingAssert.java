package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.Iterator;

public class ObjectListingAssert extends AbstractAssert<ObjectListingAssert, ObjectListing> {

	private ObjectListingAssert(ObjectListing actual) {
		super(actual, ObjectListingAssert.class);
	}

	public static ObjectListingAssert assertThat(ObjectListing objectListing) {
		return new ObjectListingAssert(objectListing);
	}

	public void isNotTruncated() {
		Assertions.assertThat(actual.isTruncated()).isFalse();
	}

	public ObjectListingAssert isTruncated() {
		Assertions.assertThat(actual.isTruncated()).isTrue();
		return this;
	}

	public ObjectListingAssert isEqualTo(ObjectListing expected) {
		Assertions.assertThat(actual.getBucketName()).isEqualTo(expected.getBucketName());
		Assertions.assertThat(actual.getDelimiter()).isEqualTo(expected.getDelimiter());
		Assertions.assertThat(actual.getMarker()).isEqualTo(expected.getMarker());
		Assertions.assertThat(actual.getMaxKeys()).isEqualTo(expected.getMaxKeys());
		Assertions.assertThat(actual.getNextMarker()).isEqualTo(expected.getNextMarker());
		Assertions.assertThat(actual.getPrefix()).isEqualTo(expected.getPrefix());

		Assertions.assertThat(actual.getCommonPrefixes()).isEqualTo(expected.getCommonPrefixes());
		Assertions.assertThat(actual.getObjectSummaries()).hasSameSizeAs(expected.getObjectSummaries());

		Iterator<S3ObjectSummary> iterator = actual.getObjectSummaries().iterator();
		Iterator<S3ObjectSummary> amazonIterator = expected.getObjectSummaries().iterator();

		while (iterator.hasNext()) {
			Assertions.assertThat(iterator.next()).isEqualToComparingFieldByField(amazonIterator.next());
		}

		return this;
	}

	public ObjectListingAssert hasSize(int expected) {
		Assertions.assertThat(actual.getObjectSummaries()).hasSize(expected);
		return this;
	}
}
