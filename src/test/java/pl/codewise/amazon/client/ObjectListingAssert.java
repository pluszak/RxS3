package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;

import java.util.Iterator;
import java.util.List;

public class ObjectListingAssert extends AbstractAssert<ObjectListingAssert, ObjectListing> {

	private String[] fieldsToIgnore = new String[0];

	ObjectListingAssert(ObjectListing actual) {
		super(actual, ObjectListingAssert.class);
	}

	public void isNotTruncated() {
		Assertions.assertThat(actual.isTruncated()).isFalse();
	}

	public ObjectListingAssert isTruncated() {
		Assertions.assertThat(actual.isTruncated()).isTrue();
		return this;
	}

	public ObjectListingAssert isEqualTo(ObjectListing expected) {
		Assertions.assertThat(actual.getBucketName())
				.overridingErrorMessage("BucketName mismatch. Expected %s, actual: %s", expected.getBucketName(), actual.getBucketName())
				.isEqualTo(expected.getBucketName());

		Assertions.assertThat(actual.getDelimiter())
				.overridingErrorMessage("Delimiter mismatch. Expected %s, actual: %s", expected.getDelimiter(), actual.getDelimiter())
				.isEqualTo(expected.getDelimiter());

		Assertions.assertThat(actual.getMarker())
				.overridingErrorMessage("Marker mismatch. Expected %s, actual: %s", expected.getMarker(), actual.getMarker())
				.isEqualTo(expected.getMarker());

		Assertions.assertThat(actual.getMaxKeys())
				.overridingErrorMessage("MaxKeys mismatch. Expected %s, actual: %s", expected.getMaxKeys(), actual.getMaxKeys())
				.isEqualTo(expected.getMaxKeys());

		Assertions.assertThat(actual.getNextMarker())
				.overridingErrorMessage("NextMarker mismatch. Expected %s, actual: %s", expected.getNextMarker(), actual.getNextMarker())
				.isEqualTo(expected.getNextMarker());

		Assertions.assertThat(actual.getPrefix())
				.overridingErrorMessage("Prefix mismatch. Expected %s, actual: %s", expected.getPrefix(), actual.getPrefix())
				.isEqualTo(expected.getPrefix());

		Assertions.assertThat(actual.getCommonPrefixes())
				.overridingErrorMessage("CommonPrefixes mismatch. Expected %s, actual: %s", expected.getCommonPrefixes(), actual.getCommonPrefixes())
				.isEqualTo(expected.getCommonPrefixes());

		Assertions.assertThat(actual.getObjectSummaries())
				.overridingErrorMessage("ObjectSummaries mismatch. Expected %s, actual: %s", expected.getObjectSummaries(), actual.getObjectSummaries())
				.hasSameSizeAs(expected.getObjectSummaries());

		Iterator<S3ObjectSummary> iterator = actual.getObjectSummaries().iterator();
		Iterator<S3ObjectSummary> amazonIterator = expected.getObjectSummaries().iterator();

		while (iterator.hasNext()) {
			Assertions.assertThat(iterator.next())
					.isEqualToIgnoringGivenFields(amazonIterator.next(), fieldsToIgnore);
		}

		return this;
	}

	public ObjectListingAssert ignoreFields(List<String> fieldsToIgnore) {
		this.fieldsToIgnore = fieldsToIgnore.toArray(this.fieldsToIgnore);
		return this;
	}

	public ObjectListingAssert hasSize(int expected) {
		Assertions.assertThat(actual.getObjectSummaries()).hasSize(expected);
		return this;
	}
}
