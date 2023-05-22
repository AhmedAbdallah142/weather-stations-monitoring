package ddia.centralStation;

import ddia.bitcask.service.Bitcask;
import ddia.centralStation.models.BitcaskSing;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;

@RestController
@RequestMapping("/station")
public class LiveStatusService {
    private final Bitcask bitcask;

    public LiveStatusService() throws IOException {
        bitcask = BitcaskSing.getBitcask();
    }

    @GetMapping("/{stationId}")
    public ResponseEntity<?> getStationStatus(@PathVariable Long stationId) throws IOException {
        byte[] key = ByteBuffer.allocate(Long.BYTES).putLong(stationId).array();
        var val = bitcask.get(key);
        return ResponseEntity.ok(val == null ? null : new String(val));
    }

    @GetMapping("")
    public ResponseEntity<?> getAllStatus() throws IOException {
        List<String> status = new LinkedList<>();
        for (var key : bitcask.listKeys()) {
            status.add(new String(bitcask.get(key)));
        }
        return ResponseEntity.ok(status);
    }


}
