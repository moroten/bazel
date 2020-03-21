// Copyright 2020 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.devtools.build.lib.remote;

import com.google.common.base.Preconditions;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.FileArtifactValue;
import com.google.devtools.build.lib.actions.MetadataProvider;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.cache.VirtualActionInput;
import com.google.devtools.build.lib.collect.nestedset.Order;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.util.Fingerprint;
import java.io.IOException;

/** A fast action input digest calculator for remote caching and execution. */
@ThreadSafe
class FastInputKeyCache {
  private Cache<Object, byte[]> inputObjectHashCache;

  public FastInputKeyCache() {
    this.inputObjectHashCache = CacheBuilder.newBuilder().weakKeys().build();
  }

  public void calcDigest(Fingerprint fp, Spawn spawn, MetadataProvider metadataProvider) throws IOException {
    // Need stable order when using Fingerprint instead of xor.
    // Avoiding xor so that double entries don't cancel each other.
    try (SilentCloseable c = Profiler.instance().profile("FastInputKeyCache.calcDigest")) {
      Preconditions.checkArgument(spawn.getInputFiles().getOrder() == Order.STABLE_ORDER);
      Object node = spawn.getInputFiles().getChildrenUnsafe();
      calcNestedSetObjectDigest(node, fp, metadataProvider);
    }
  }

  private void calcNestedSetObjectDigest(Object node, Fingerprint fp, MetadataProvider metadataProvider) throws IOException {
    // In src/main/java/com/google/devtools/build/lib/collect/nestedset/NestedSetVisitor.java
    // see visitRaw() for loop iteration.
    // In src/main/java/com/google/devtools/build/lib/actions/cache/DigestUtils.java
    // see fromMetadata() and getDigest() for hash calculation.
    byte[] result = new byte[1]; // reserve the empty string
    if (node instanceof Object[]) {
      byte[] childDigest = inputObjectHashCache.getIfPresent(node);
      if (childDigest == null) {
        Fingerprint subfp = new Fingerprint();
        for (Object child : (Object[]) node) {
          calcNestedSetObjectDigest(child, subfp, metadataProvider);
        }
        childDigest = subfp.digestAndReset();
        inputObjectHashCache.put(node, childDigest);
      }
      fp.addBytes(childDigest);
    } else {
      ActionInput input = (ActionInput) node;
      if (input instanceof VirtualActionInput) {
        VirtualActionInput virtualActionInput = (VirtualActionInput) input;
        fp.addString(virtualActionInput.getExecPathString());
        fp.addBytes(virtualActionInput.getBytes());
      } else {
        FileArtifactValue metadata =
            Preconditions.checkNotNull(
                metadataProvider.getMetadata(input),
                "missing metadata for '%s'",
                input.getExecPathString());
        fp.addString(input.getExecPathString());
        if (metadata == null) {
          // Move along, nothing to see here.
        } else if (metadata.getDigest() != null) {
          fp.addBytes(metadata.getDigest());
        } else {
          // Use the timestamp if the digest is not present, but not both. Modifying a timestamp while
          // keeping the contents of a file the same should not cause rebuilds.
          fp.addLong(metadata.getModifiedTime());
        }
      }
    }
  }
}
