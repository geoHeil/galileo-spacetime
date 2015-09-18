package galileo.test.fs;

import galileo.util.PerformanceTimer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;

public class ShmTest {

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println("usage: ShmTest <filename> <size (MB)> <iters>");
            System.exit(1);
        }

        int mb = Integer.parseInt(args[1]);
        int iters = Integer.parseInt(args[2]);

        for (int i = 0; i < iters; ++i) {
            File f = new File(args[0] + "." + i);
            testWrite(f, 1000000 * mb);
        }

        for (int i = 0; i < iters; ++i) {
            File f = new File(args[0] + "." + i);
            testRead(f);
        }
    }

    private static void testWrite(File file, int size) throws Exception {
        PerformanceTimer pt = new PerformanceTimer("write");
        Random r = new Random(System.nanoTime());
        byte[] fileData = new byte[size];
        r.nextBytes(fileData);
        pt.start();
        Files.write(file.toPath(), fileData, StandardOpenOption.CREATE);
        pt.stopAndPrint();
    }

    private static byte[] testRead(File file) throws Exception {
        PerformanceTimer pt = new PerformanceTimer("read");
        pt.start();
        byte[] fileData = Files.readAllBytes(file.toPath());
        pt.stopAndPrint();
        return fileData;
    }
}
