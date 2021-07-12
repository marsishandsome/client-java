/*
 *
 * Copyright 2021 PingCAP, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.tikv.common.importer;

import com.google.protobuf.ByteString;
import io.grpc.stub.StreamObserver;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.tikv.common.TiConfiguration;
import org.tikv.common.TiSession;
import org.tikv.common.key.Key;
import org.tikv.common.region.TiRegion;
import org.tikv.common.region.TiStore;
import org.tikv.common.util.Pair;
import org.tikv.kvproto.ImportSstpb;
import org.tikv.kvproto.Metapb;

public class ImporterClient {
  private TiConfiguration tiConf;
  private TiSession tiSession;
  private byte[] uuid;
  private Key minKey;
  private Key maxKey;
  private TiRegion region;

  private boolean openStream = false;
  private ImportSstpb.SSTMeta sstMeta;
  private List<ImporterStoreClient> clientList;
  private ImporterStoreClient clientLeader;

  // TODO:  mars need refactor
  public ImporterClient(TiSession tiSession, byte[] uuid, Key minKey, Key maxKey, TiRegion region) {
    this.uuid = uuid;
    this.tiConf = tiSession.getConf();
    this.tiSession = tiSession;
    this.minKey = minKey;
    this.maxKey = maxKey;
    this.region = region;
  }

  /**
   * write KV pairs to RawKV using KVStream interface
   *
   * @param iterator
   */
  public void rawWrite(Iterator<Pair<ByteString, ByteString>> iterator) {
    if (!tiConf.isRawKVMode()) {
      throw new IllegalArgumentException("KVMode is not RAW in TiConfiguration!");
    }

    openStream = false;

    int maxKVBatchSize = tiConf.getImporterMaxKVBatchSize();
    int maxKVBatchBytes = tiConf.getImporterMaxKVBatchBytes();
    int totalBytes = 0;
    while (iterator.hasNext()) {
      ArrayList<ImportSstpb.Pair> pairs = new ArrayList<>(maxKVBatchSize);
      for (int i = 0; i < maxKVBatchSize; i++) {
        if (iterator.hasNext()) {
          Pair<ByteString, ByteString> pair = iterator.next();
          pairs.add(ImportSstpb.Pair.newBuilder().setKey(pair.first).setValue(pair.second).build());
          totalBytes += (pair.first.size() + pair.second.size());
        }
        if (totalBytes > maxKVBatchBytes) {
          break;
        }
      }
      if (!openStream) {
        init();
        openStream();
        writeMeta();
        openStream = true;
      }
      writeBatch(pairs);
    }

    if (openStream) {
      finishWriteBatch();
      ingest();
    }
  }

  private void init() {
    long regionId = region.getId();
    Metapb.RegionEpoch regionEpoch = region.getRegionEpoch();
    ImportSstpb.Range range =
        ImportSstpb.Range.newBuilder()
            .setStart(minKey.toByteString())
            .setEnd(maxKey.toByteString())
            .build();

    sstMeta =
        ImportSstpb.SSTMeta.newBuilder()
            .setUuid(ByteString.copyFrom(uuid))
            .setRegionId(regionId)
            .setRegionEpoch(regionEpoch)
            .setRange(range)
            .build();

    clientLeader = tiSession.getImporterRegionStoreClientBuilder().build(region);
    clientList = new ArrayList<>();
    for (Metapb.Peer peer : region.getPeersList()) {
      long storeId = peer.getStoreId();
      TiStore store = tiSession.getRegionManager().getStoreById(storeId);
      clientList.add(tiSession.getImporterRegionStoreClientBuilder().build(store));
    }
  }

  private void openStream() {
    for (ImporterStoreClient client : clientList) {
      RawWriteObserver streamObserverResponse = new RawWriteObserver(client);
      StreamObserver<ImportSstpb.RawWriteRequest> streamObserverRequest =
          client.rawWrite(streamObserverResponse);

      client.streamObserverResponse = streamObserverResponse;
      client.streamObserverRequest = streamObserverRequest;
    }
  }

  private void writeMeta() {
    ImportSstpb.RawWriteRequest request =
        ImportSstpb.RawWriteRequest.newBuilder().setMeta(sstMeta).build();
    for (ImporterStoreClient client : clientList) {
      client.streamObserverRequest.onNext(request);
    }
  }

  private void writeBatch(List<ImportSstpb.Pair> pairs) {
    ImportSstpb.RawWriteBatch batch =
        ImportSstpb.RawWriteBatch.newBuilder().addAllPairs(pairs).build();
    ImportSstpb.RawWriteRequest request =
        ImportSstpb.RawWriteRequest.newBuilder().setBatch(batch).build();
    for (ImporterStoreClient client : clientList) {
      client.streamObserverRequest.onNext(request);
    }
  }

  private void finishWriteBatch() {
    for (ImporterStoreClient client : clientList) {
      client.streamObserverRequest.onCompleted();
    }
  }

  private void ingest() {
    ImportSstpb.RawWriteResponse response = null;
    int returnNumber = 0;
    while (returnNumber < clientList.size()) {
      returnNumber = 0;
      for (ImporterStoreClient client : clientList) {
        response = client.getRawWriteResponse();
        if (response != null) {
          returnNumber++;
        }
      }

      if (returnNumber < clientList.size()) {
        try {
          Thread.sleep(1000);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
      }
    }

    clientLeader.multiIngest(region.getLeaderContext(), response.getMetasList());
  }
}
