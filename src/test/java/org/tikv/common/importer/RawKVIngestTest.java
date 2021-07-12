package org.tikv.common.importer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.util.Pair;
import org.tikv.raw.RawKVClient;
import org.tikv.util.TestUtils;

public class RawKVIngestTest {
  private TiSession session;

  private final int keyNumber = 16;
  private final String keyPrefix = "prefix_rawkv_ingest_test_";
  private final int keyLength = keyPrefix.length() + 10;
  private final int valueLength = 16;

  @Before
  public void setup() {
    TiConfiguration conf = TiConfiguration.createRawDefault();
    session = TiSession.create(conf);
  }

  @After
  public void tearDown() throws Exception {
    if (session != null) {
      session.close();
    }
  }

  @Test
  public void rawKVIngestTest() {
    RawKVClient client = session.createRawClient();

    // gen test data
    List<Pair<ByteString, ByteString>> sortedList = new ArrayList<>();
    for (int i = 0; i < keyNumber; i++) {
      byte[] key = TestUtils.genRandomKey(keyPrefix, keyLength);
      byte[] value = TestUtils.genRandomValue(valueLength);
      sortedList.add(Pair.create(ByteString.copyFrom(key), ByteString.copyFrom(value)));
    }
    sortedList.sort(
        (o1, o2) -> {
          Key k1 = Key.toRawKey(o1.first.toByteArray());
          Key k2 = Key.toRawKey(o2.first.toByteArray());
          return k1.compareTo(k2);
        });

    // ingest
    client.ingest(sortedList);

    // assert
    for (Pair<ByteString, ByteString> pair : sortedList) {
      Optional<ByteString> v = client.get(pair.first);
      assertTrue(v.isPresent());
      assertEquals(deleteTail8Bytes(v.get()), pair.second);
    }
  }

  // TODO: mars because of ttl bug
  private ByteString deleteTail8Bytes(ByteString bs) {
    return bs.substring(0, bs.size() - 8);
  }
}
