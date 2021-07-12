package org.tikv.common.importer;

import static org.junit.Assert.assertArrayEquals;
import static org.tikv.util.TestUtils.genRandomKey;

import com.google.protobuf.ByteString;
import java.util.ArrayList;
import java.util.List;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.region.TiRegion;

// TODO: mars wait for these PR to merge
// https://github.com/tikv/tikv/pull/10524/
// https://github.com/pingcap/kvproto/pull/779
// http://fileserver.pingcap.net/download/builds/pingcap/tikv/pr/c94cbf4a18c458b5d3c14818576a8cb0274ac736/centos7/tikv-server.tar.gz
public class RegionSplitTest {
  private TiSession session;

  private final int keyNumber = 10;
  private final String keyPrefix = "prefix_region_split_test_";
  private final int keyLength = keyPrefix.length() + 10;

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
  public void rawKVSplitTest() {
    List<byte[]> splitKeys = new ArrayList<>(keyNumber);
    for (int i = 0; i < keyNumber; i++) {
      splitKeys.add(genRandomKey(keyPrefix, keyLength));
    }

    session.splitRegionAndScatter(splitKeys);
    session.getRegionManager().invalidateAll();

    for (int i = 0; i < keyNumber; i++) {
      byte[] key = splitKeys.get(i);
      TiRegion region = session.getRegionManager().getRegionByKey(ByteString.copyFrom(key));
      assertArrayEquals(key, region.getStartKey().toByteArray());
    }
  }
}
