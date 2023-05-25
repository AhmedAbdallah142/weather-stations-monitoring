package ddia.centralStation.models;

import ddia.bitcask.service.Bitcask;
import ddia.bitcask.service.Impl.BitcaskImpl;
import ddia.centralStation.config.Props;

import java.io.IOException;

public class BitcaskSing {
    private final Bitcask bitcask;
    private static BitcaskSing bitcaskSing;
    private static final String bitcaskDir = Props.DATA_PATH + "/bitcask";
    private BitcaskSing() throws IOException {
        this.bitcask = new BitcaskImpl(bitcaskDir);
    }

    public static synchronized Bitcask getBitcask() throws IOException {
        if(bitcaskSing == null)
            bitcaskSing = new BitcaskSing();
        return bitcaskSing.bitcask;
    }
}
