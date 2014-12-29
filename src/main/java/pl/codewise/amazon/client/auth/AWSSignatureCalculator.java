package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentials;
import com.google.common.collect.Iterables;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilderBase;
import com.ning.http.client.SignatureCalculator;
import com.ning.http.util.Base64;
import javolution.text.TextBuilder;
import org.apache.commons.lang3.time.FastDateFormat;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import pl.codewise.amazon.client.AsyncS3Client;

import java.util.Arrays;
import java.util.List;
import java.util.Locale;

public class AWSSignatureCalculator implements SignatureCalculator {

	private static final FastDateFormat RFC_822_DATE_FORMAT = FastDateFormat.getInstance("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);

	public final static String HEADER_AUTHORIZATION = "Authorization";
	public final static String HEADER_DATE = "Date";

	private final String accessKey;
	private final KeyParameter keyParameter;

	private final HMac hmac = new HMac(new SHA1Digest());
	private final byte[] signingResultHolder = new byte[hmac.getMacSize()];
	private final TextBuilder stringToSignBuilder = new TextBuilder();

	private final Operation operation;

	public AWSSignatureCalculator(String accessKey, String secretKey, Operation operation) {
		this.accessKey = accessKey;
		this.keyParameter = new KeyParameter(secretKey.getBytes());

		this.operation = operation;
	}

	public AWSSignatureCalculator(AWSCredentials credentials, Operation operation) {
		this(credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey(), operation);
	}

	@SuppressWarnings("StringBufferReplaceableByString")
	@Override
	public void calculateAndAddSignature(String url, Request request, RequestBuilderBase<?> requestBuilder) {
		try {
			calculateAndAddSignature(request, requestBuilder);
		} finally {
			stringToSignBuilder.clear();
			Arrays.fill(signingResultHolder, (byte) 0);
			hmac.reset();
		}
	}

	private void calculateAndAddSignature(Request request, RequestBuilderBase<?> requestBuilder) {
		String contentMd5 = "";
		String contentType = "";

		List<String> strings = request.getHeaders().get("Content-MD5");
		if (strings != null && !strings.isEmpty()) {
			contentMd5 = Iterables.getLast(strings);
		}

		strings = request.getHeaders().get("Content-Type");
		if (strings != null && !strings.isEmpty()) {
			contentType = Iterables.getLast(strings);
		}

		String dateString = RFC_822_DATE_FORMAT.format(System.currentTimeMillis());
		stringToSignBuilder
				.append(operation.getOperationName())
				.append('\n')
				.append(contentMd5)
				.append('\n')
				.append(contentType)
				.append('\n')
				.append(dateString)
				.append('\n')
				.append('/');

		String virtualHost = request.getVirtualHost();
		stringToSignBuilder.append(virtualHost, 0, virtualHost.length() - AsyncS3Client.S3_LOCATION.length() - 1);
		operation.getResourceName(stringToSignBuilder, request);

		String authorization = calculateRFC2104HMAC(stringToSignBuilder.toString(), keyParameter);
		stringToSignBuilder.clear();

		stringToSignBuilder.append("AWS ").append(accessKey).append(':').append(authorization);

		requestBuilder.addHeader(HEADER_AUTHORIZATION, stringToSignBuilder.toString());
		requestBuilder.addHeader(HEADER_DATE, dateString);
	}

	public String calculateRFC2104HMAC(String stringToSign, KeyParameter keyParameter) {
		hmac.init(keyParameter);

		byte[] stringToSignBytes = stringToSign.getBytes();
		hmac.update(stringToSignBytes, 0, stringToSignBytes.length);

		hmac.doFinal(signingResultHolder, 0);
		return Base64.encode(signingResultHolder);
	}
}
