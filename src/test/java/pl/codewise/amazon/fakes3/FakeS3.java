package pl.codewise.amazon.fakes3;

import org.assertj.core.api.Assertions;

import java.io.Closeable;
import java.io.IOException;
import java.net.ServerSocket;

public class FakeS3 implements Closeable {

	private Process fakeS3Process;
	private int localPort;

	public FakeS3() {
		localPort = 0;

		try (ServerSocket serverSocket = new ServerSocket(0)) {
			localPort = serverSocket.getLocalPort();
		} catch (IOException e) {
			Assertions.fail("Error while searching for free port", e);
		}

		try {
			fakeS3Process = new ProcessBuilder().command("fakes3", "-r", "/mnt/fakes3", "-p", Integer.toString(localPort)).start();
		} catch (IOException e) {
			Assertions.fail("Error while starting fakeS3 process", e);
		}
	}

	public int getLocalPort() {
		return localPort;
	}

	@Override
	public void close() {
		fakeS3Process.destroy();
	}
}
