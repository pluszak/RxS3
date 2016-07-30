package pl.codewise.amazon.client;

import com.amazonaws.services.s3.model.ListObjectsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import javolution.text.TextBuilder;
import pl.codewise.amazon.client.utils.UTF8UrlEncoder;

public class RestUtils {

    public static TextBuilder appendQueryString(TextBuilder result, ListObjectsRequest listObjectsRequest) {
        return appendQueryString(result, listObjectsRequest.getPrefix(), listObjectsRequest.getMarker(), listObjectsRequest.getDelimiter(), listObjectsRequest.getMaxKeys());
    }

    public static TextBuilder appendQueryString(TextBuilder result, ObjectListing objectListing) {
        return appendQueryString(result, objectListing.getPrefix(), objectListing.getMarker(), objectListing.getDelimiter(), objectListing.getMaxKeys());
    }

    public static TextBuilder appendQueryString(TextBuilder result, CharSequence prefix, CharSequence marker, CharSequence delimiter, Integer maxKeys) {
        if (prefix != null) {
            result.append("prefix=");
            UTF8UrlEncoder.appendEncoded(result, prefix, 0);
        }

        if (marker != null) {
            if (result.length() > 0) {
                result.append("&");
            }

            result.append("marker=");
            UTF8UrlEncoder.appendEncoded(result, marker, 0);
        }

        if (delimiter != null) {
            if (result.length() > 0) {
                result.append("&");
            }

            result.append("delimiter=");
            UTF8UrlEncoder.appendEncoded(result, delimiter, 0);
        }

        if (maxKeys != null) {
            if (result.length() > 0) {
                result.append("&");
            }

            result.append("max-keys=");
            UTF8UrlEncoder.appendEncoded(result, maxKeys.toString(), 0);
        }

        return result;
    }
}
