package galileo.test.fs;

import galileo.util.PerformanceTimer;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.Random;
import java.util.concurrent.FutureTask;

public class DiskBenchmark {

    private static final int dataSize = 250000000;

    private byte data1[] = new byte[dataSize];
    private byte data2[] = new byte[dataSize];

    private class RandomWorker implements Runnable {
        private byte[] target;

        public RandomWorker(byte[] target) {
            this.target = target;
        }

        public void run() {
            Random rand = new Random();
            rand.nextBytes(target);
        }
    }

    public static void main(String[] args) throws Exception {
        DiskBenchmark db = new DiskBenchmark();
        db.run(args[0]);
    }

    public DiskBenchmark() {
        System.out.println("Generating data...");
        Random rand = new Random();
        rand.nextBytes(data1);
        rand.nextBytes(data2);
    }

    public void run(String fileName) throws Exception {
        byte[] dataA = data1;
        byte[] dataB = data2;
        double mb = dataSize / 1000000;

        while (true) {
            FutureTask<String> ft = new FutureTask<>(
                    new RandomWorker(dataB), "");
            ft.run();

            System.out.println("Writing " + fileName);
            PerformanceTimer pt = new PerformanceTimer("Write");
            pt.start();
            Files.write(
                    new File(fileName).toPath(),
                    dataA,
                    StandardOpenOption.CREATE);
            pt.stop();
            double time = pt.getLastResult();
            double speed = mb / (time * 1e-3);
            System.out.println(speed);

            if (ft.isDone() == false) {
                System.out.println("THREAD CANCELED!");
                ft.cancel(true);
            }

            byte[] temp = dataA;
            dataA = dataB;
            dataB = temp;
        }
    }
}
