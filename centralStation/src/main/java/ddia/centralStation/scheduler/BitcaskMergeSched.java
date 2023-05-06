package ddia.centralStation.scheduler;

import ddia.centralStation.models.BitcaskSing;

public class BitcaskMergeSched implements Runnable{

    private static final long dely = 1000L * 60 * 60 * 24;
    @Override
    public void run() {
        while (true) {
            try {
                Thread.sleep(dely);
                BitcaskSing.getBitcask().merge();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

    }
}
