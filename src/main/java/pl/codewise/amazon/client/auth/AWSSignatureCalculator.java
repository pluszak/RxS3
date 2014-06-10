package pl.codewise.amazon.client.auth;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.services.s3.internal.ServiceUtils;
import com.google.common.collect.Iterables;
import com.ning.http.client.Request;
import com.ning.http.client.RequestBuilderBase;
import com.ning.http.client.SignatureCalculator;
import com.ning.http.util.Base64;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.List;

public class AWSSignatureCalculator implements SignatureCalculator {

	private static final Logger LOGGER = LoggerFactory.getLogger(AWSSignatureCalculator.class);

	public final static String HEADER_AUTHORIZATION = "Authorization";
	public final static String HEADER_DATE = "Date";

	private final String accessKey;
	private final String secretKey;

	private Operation operation;

	public AWSSignatureCalculator(String accessKey, String secretKey, Operation operation) {
		this.accessKey = accessKey;
		this.secretKey = secretKey;
		this.operation = operation;
	}

	public AWSSignatureCalculator(AWSCredentials credentials, Operation operation) {
		this(credentials.getAWSAccessKeyId(), credentials.getAWSSecretKey(), operation);
	}

	@SuppressWarnings("StringBufferReplaceableByString")
	@Override
	public void calculateAndAddSignature(String url, Request request, RequestBuilderBase<?> requestBuilder) {
		Date date = new Date();

		String contentMd5 = "";
		List<String> strings = request.getHeaders().get("Content-MD5");
		if (strings != null && !strings.isEmpty()) {
			contentMd5 = Iterables.getLast(strings);
		}
		String dateString = ServiceUtils.formatRfc822Date(date);
		String stringToSign = new StringBuilder().append(operation.getOperationName())
				.append("\n")
				.append(contentMd5)
				.append("\n")
				.append("\n")
				.append(dateString).append("\n")
				.append(operation.getResourceName(request)).toString();

		LOGGER.debug("String to sign: {}", stringToSign);
		String authorization = calculateRFC2104HMAC(stringToSign, secretKey);

		requestBuilder.addHeader(HEADER_DATE, dateString);
		requestBuilder.addHeader(HEADER_AUTHORIZATION, "AWS " + accessKey + ":" + authorization);
	}

	public static String calculateRFC2104HMAC(String data, String key) {
		HMac hmac = new HMac(new SHA1Digest());

		KeyParameter keyParameter = new KeyParameter(key.getBytes());
		hmac.init(keyParameter);

		byte[] bytes = data.getBytes();
		hmac.update(data.getBytes(), 0, bytes.length);

		byte[] result = new byte[hmac.getMacSize()];
		hmac.doFinal(result, 0);

		return Base64.encode(result);
	}
}
