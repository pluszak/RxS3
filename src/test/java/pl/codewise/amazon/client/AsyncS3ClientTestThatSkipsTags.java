package pl.codewise.amazon.client;

import org.junit.BeforeClass;

import java.io.IOException;
import java.util.Arrays;

public class AsyncS3ClientTestThatSkipsTags extends AsyncS3ClientTest {

	@BeforeClass
	public static void setUpConfigurationThatDoesNotSkipTags() throws IOException {
		fieldsToIgnore.addAll(Arrays.asList("lastModified", "storageClass", "owner"));

		configuration = ClientConfiguration
				.builder()
				.skipParsingETag()
				.skipParsingLasModified()
				.skipParsingStorageClass()
				.skipParsingOwner()
				.useCredentials(credentials)
				.build();
	}
}
