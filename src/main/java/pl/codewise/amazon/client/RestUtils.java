package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.google.common.base.Joiner;
import com.google.common.collect.Maps;

import java.io.UnsupportedEncodingException;
import java.net.URLEncoder;
import java.util.Map;

public class RestUtils {

	public static String createQueryString(ListObjectsRequest listObjectsRequest) {
		return createQueryString(listObjectsRequest.getPrefix(), listObjectsRequest.getMarker(), listObjectsRequest.getDelimiter(), listObjectsRequest.getMaxKeys());
	}

	public static String createQueryString(ObjectListing objectListing) {
		return createQueryString(objectListing.getPrefix(), objectListing.getMarker(), objectListing.getDelimiter(), objectListing.getMaxKeys());
	}

	public static String createQueryString(String prefix, String marker, String delimiter, Integer maxKeys) {
		Map<String, String> result = Maps.newLinkedHashMap();
		if (prefix != null) {
			result.put("prefix", escape(prefix));
		}

		if (marker != null) {
			result.put("marker", escape(marker));
		}

		if (delimiter != null) {
			result.put("delimiter", escape(delimiter));
		}

		if (maxKeys != null) {
			result.put("max-keys", maxKeys.toString());
		}

		return Joiner.on("&").withKeyValueSeparator("=").join(result);
	}

	public static String escape(String prefix) {
		try {
			return URLEncoder.encode(prefix, "UTF-8");
		} catch (UnsupportedEncodingException e) {
			throw new RuntimeException(e);
		}
	}
}
