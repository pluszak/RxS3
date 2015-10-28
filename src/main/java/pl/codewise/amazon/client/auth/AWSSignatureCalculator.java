package pl.codewise.amazon.client.auth;

import java.util.Arrays;
import java.util.Base64;
import java.util.Locale;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSSessionCredentials;
import io.netty.handler.codec.http.HttpHeaders;
import javolution.text.TextBuilder;
import org.apache.commons.lang3.time.FastDateFormat;
import org.bouncycastle.crypto.digests.SHA1Digest;
import org.bouncycastle.crypto.macs.HMac;
import org.bouncycastle.crypto.params.KeyParameter;

public class AWSSignatureCalculator implements SignatureCalculator {

    private static final FastDateFormat RFC_822_DATE_FORMAT = FastDateFormat.getInstance("EEE, dd MMM yyyy HH:mm:ss z", Locale.US);

    public final static String HEADER_AUTHORIZATION = "Authorization";
    public final static String HEADER_TOKEN = "x-amz-security-token";
    public final static String HEADER_DATE = "Date";

    private final String s3Location;

    private final AWSCredentialsProvider credentialsProvider;

    private final HMac hmac = new HMac(new SHA1Digest());
    private final byte[] signingResultHolder = new byte[hmac.getMacSize()];

    private final TextBuilder stringToSignBuilder = new TextBuilder();
    private final Operation operation;

    public AWSSignatureCalculator(AWSCredentialsProvider credentialsProvider, Operation operation, String s3Location) {
        this.credentialsProvider = credentialsProvider;
        this.s3Location = s3Location;

        this.operation = operation;
    }

    @Override
    public void calculateAndAddSignature(HttpHeaders httpHeaders, String objectName, String contentMd5, String contentType, String virtualHost) {
        try {
            calculateAndAddSignatureInternal(httpHeaders, objectName, contentMd5, contentType, virtualHost);
        } finally {
            stringToSignBuilder.clear();
            Arrays.fill(signingResultHolder, (byte) 0);
            hmac.reset();
        }
    }

    private void calculateAndAddSignatureInternal(HttpHeaders httpHeaders, String objectName, String contentMd5, String contentType, String virtualHost) {
        String dateString = RFC_822_DATE_FORMAT.format(System.currentTimeMillis());
        stringToSignBuilder
                .append(operation.getOperationName())
                .append('\n')
                .append(contentMd5)
                .append('\n')
                .append(contentType)
                .append('\n')
                .append(dateString)
                .append('\n');

        AWSCredentials credentials = credentialsProvider.getCredentials();
        if (credentials instanceof AWSSessionCredentials) {
            String sessionToken = ((AWSSessionCredentials) credentials).getSessionToken();
            httpHeaders.set(HEADER_TOKEN, sessionToken);
            stringToSignBuilder
                    .append(HEADER_TOKEN)
                    .append(':')
                    .append(sessionToken)
                    .append('\n');
        }

        stringToSignBuilder.append('/');

        stringToSignBuilder.append(virtualHost);
        operation.getResourceName(stringToSignBuilder, objectName);

        KeyParameter keyParameter = new KeyParameter(credentials.getAWSSecretKey().getBytes());
        String authorization = calculateRFC2104HMAC(stringToSignBuilder.toString(), keyParameter);
        stringToSignBuilder.clear();

        stringToSignBuilder.append("AWS ").append(credentials.getAWSAccessKeyId()).append(':').append(authorization);

        httpHeaders.set(HEADER_AUTHORIZATION, stringToSignBuilder.toString());
        httpHeaders.set(HEADER_DATE, dateString);
    }

    public String calculateRFC2104HMAC(String stringToSign, KeyParameter keyParameter) {
        hmac.init(keyParameter);

        byte[] stringToSignBytes = stringToSign.getBytes();
        hmac.update(stringToSignBytes, 0, stringToSignBytes.length);

        hmac.doFinal(signingResultHolder, 0);
        return Base64.getEncoder().encodeToString(signingResultHolder);
    }
}
