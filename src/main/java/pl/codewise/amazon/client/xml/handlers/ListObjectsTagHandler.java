package pl.codewise.amazon.client.xml.handlers;

import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.Owner;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.xmlpull.v1.XmlPullParser;

import java.util.LinkedList;
import java.util.List;

public enum ListObjectsTagHandler implements TagHandler<ObjectListing> {

	LIST_BUCKET_RESULT("ListBucketResult") {
		@Override
		public void handleEnd(ObjectListing objectListing, XmlPullParser parser) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			if (objectSummaries.size() > 0) {
				S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);
				objectListing.setNextMarker(summary.getKey());
			}
		}
	}, IS_TRUNCATED("IsTruncated") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			objectListing.setTruncated(Boolean.parseBoolean(parser.getText()));
		}
	}, KEY("Key") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			summary.setKey(parser.getText());
		}
	}, ETAG("ETag") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			summary.setETag(parser.getText());
		}
	}, SIZE("Size") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			summary.setSize(Long.parseLong(parser.getText()));
		}
	}, LAST_MODIFIED("LastModified") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			LocalDateTime dateTime = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSSZ").parseLocalDateTime(parser.getText());
			summary.setLastModified(dateTime.toDate());
		}
	}, STORAGE_CLASS("StorageClass") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			summary.setStorageClass(parser.getText());
		}
	}, OWNER("Owner") {
		@Override
		public void handleStart(ObjectListing objectListing, XmlPullParser parser) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			summary.setOwner(new Owner());
		}
	}, ID("ID") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			Owner owner = summary.getOwner();
			owner.setId(parser.getText());
		}
	}, DISPLAY_NAME("DisplayName") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			List<S3ObjectSummary> objectSummaries = objectListing.getObjectSummaries();
			S3ObjectSummary summary = objectSummaries.get(objectSummaries.size() - 1);

			Owner owner = summary.getOwner();
			owner.setDisplayName(parser.getText());
		}
	}, CONTENTS("Contents") {
		@Override
		public void handleStart(ObjectListing objectListing, XmlPullParser parser) {
			S3ObjectSummary summary = new S3ObjectSummary();
			summary.setBucketName(objectListing.getBucketName());

			objectListing.getObjectSummaries().add(summary);
		}
	}, NAME("Name") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			objectListing.setBucketName(parser.getText());
		}
	}, PREFIX("Prefix") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			if (handlerStack.get(handlerStack.size() - 2) == COMMON_PREFIXES) {
				objectListing.getCommonPrefixes().add(parser.getText());
			} else {
				objectListing.setPrefix(parser.getText());
			}
		}
	}, MAX_KEYS("MaxKeys") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			objectListing.setMaxKeys(Integer.parseInt(parser.getText()));
		}
	}, DELIMITER("Delimiter") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			objectListing.setDelimiter(parser.getText());
		}
	}, MARKER("Marker") {
		@Override
		public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
			objectListing.setMarker(parser.getText());
		}
	}, COMMON_PREFIXES("CommonPrefixes") {
	},
	UNKNOWN("Unknown");

	private String tagName;

	ListObjectsTagHandler(String tagName) {
		this.tagName = tagName;
	}

	@Override
	public String getTagName() {
		return tagName;
	}

	public void handleText(ObjectListing objectListing, XmlPullParser parser, LinkedList<? extends TagHandler<ObjectListing>> handlerStack) {
	}

	public void handleStart(ObjectListing objectListing, XmlPullParser parser) {
	}

	public void handleEnd(ObjectListing objectListing, XmlPullParser parser) {
	}
}
