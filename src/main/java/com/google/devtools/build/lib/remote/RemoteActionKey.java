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

import static com.google.devtools.build.lib.profiler.ProfilerTask.REMOTE_CACHE_CHECK;

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Digest;
import com.google.common.base.Preconditions;
import com.google.common.base.Stopwatch;
import com.google.devtools.build.lib.actions.ActionInput;
import com.google.devtools.build.lib.actions.Spawn;
import com.google.devtools.build.lib.actions.SpawnMetrics;
import com.google.devtools.build.lib.concurrent.ThreadSafety.ThreadSafe;
import com.google.devtools.build.lib.exec.SpawnRunner.SpawnExecutionContext;
import com.google.devtools.build.lib.profiler.Profiler;
import com.google.devtools.build.lib.profiler.ProfilerTask;
import com.google.devtools.build.lib.profiler.SilentCloseable;
import com.google.devtools.build.lib.remote.common.RemoteCacheClient.ActionKey;
import com.google.devtools.build.lib.remote.merkletree.MerkleTree;
import com.google.devtools.build.lib.remote.options.RemoteOptions;
import com.google.devtools.build.lib.remote.util.DigestUtil;
import com.google.devtools.build.lib.remote.util.NetworkTime;
import com.google.devtools.build.lib.remote.util.TracingMetadataUtils;
import com.google.devtools.build.lib.util.Fingerprint;
import com.google.devtools.build.lib.vfs.Path;
import com.google.devtools.build.lib.vfs.PathFragment;
import io.grpc.Context;
import java.io.IOException;
import java.util.Map;
import java.util.SortedMap;

/** Provides action key calculation for remote caching. */
@ThreadSafe
public class RemoteActionKey {

  private SortedMap<PathFragment, ActionInput> inputMap = null;
  private MerkleTree merkleTree = null;
  private ActionKey aliasActionKey = null;
  private Digest rootDigest = null;
  private ActionKey cachedActionKey = null;
  private boolean cachedActionKeyCalculated = false;
  private ActionKey actionKey = null;
  private final Stopwatch parseTime = Stopwatch.createUnstarted();

  private final SpawnExecutionContext context;
  private final Spawn spawn;
  private final DigestUtil digestUtil;
  private final RemoteCache remoteCache;
  private final FastInputKeyCache fastInputKeyCache;
  private final Path execRoot;
  private final Digest commandHash;
  private final String buildRequestId;
  private final String commandId;
  private final SpawnMetrics.Builder spawnMetrics;
  private final NetworkTime networkTime;
  private final RemoteOptions remoteOptions;
  private final boolean spawnCacheableRemotely;

  public RemoteActionKey(
      SpawnExecutionContext context,
      Spawn spawn,
      DigestUtil digestUtil,
      RemoteCache remoteCache,
      FastInputKeyCache fastInputKeyCache,
      Path execRoot,
      Digest commandHash,
      String buildRequestId,
      String commandId,
      SpawnMetrics.Builder spawnMetrics,
      NetworkTime networkTime,
      RemoteOptions remoteOptions,
      boolean spawnCacheableRemotely) {
    this.context = Preconditions.checkNotNull(context, "context");
    this.spawn = Preconditions.checkNotNull(spawn, "spawn");
    this.digestUtil = Preconditions.checkNotNull(digestUtil, "digestUtil");
    this.remoteCache = Preconditions.checkNotNull(remoteCache, "remoteCache");
    this.fastInputKeyCache = Preconditions.checkNotNull(fastInputKeyCache, "fastInputKeyCache");
    this.execRoot = Preconditions.checkNotNull(execRoot, "execRoot");
    this.commandHash = Preconditions.checkNotNull(commandHash, "commandHash");
    this.buildRequestId = Preconditions.checkNotNull(buildRequestId, "buildRequestId");
    this.commandId = Preconditions.checkNotNull(commandId, "commandId");
    this.spawnMetrics = Preconditions.checkNotNull(spawnMetrics, "spawnMetrics");
    this.networkTime = Preconditions.checkNotNull(networkTime, "networkTime");
    this.remoteOptions = Preconditions.checkNotNull(remoteOptions, "remoteOptions");
    this.spawnCacheableRemotely = spawnCacheableRemotely;
  }

  public SortedMap<PathFragment, ActionInput> getInputMap() throws IOException {
    if (inputMap == null) {
      synchronized (this) {
        if (inputMap == null) {
          parseTime.start();
          inputMap = context.getInputMapping(true);
          parseTime.stop();
          spawnMetrics.setParseTime(parseTime.elapsed());
        }
      }
    }
    return inputMap;
  }

  public MerkleTree getMerkleTree() throws IOException {
    if (merkleTree == null) {
      synchronized (this) {
        if (merkleTree == null) {
          SortedMap<PathFragment, ActionInput> localInputMap = getInputMap();
          parseTime.start();
          merkleTree = MerkleTree.build(localInputMap, context.getMetadataProvider(), execRoot, digestUtil);
          parseTime.stop();
          spawnMetrics.setParseTime(parseTime.elapsed());
          spawnMetrics.setInputBytes(merkleTree.getInputBytes());
          spawnMetrics.setInputFiles(merkleTree.getInputFiles());
        }
      }
    }
    return merkleTree;
  }

  public ActionKey getAliasActionKey() throws IOException {
    if (remoteOptions.remoteAcceptActionAliases && spawnCacheableRemotely && aliasActionKey == null) {
      synchronized (this) {
        if (aliasActionKey == null) {
          parseTime.start();
          Fingerprint fp = new Fingerprint();
          // TODO: Make the hash and storage method stable, e.g. standardize in RE API.
          fp.addString("Version 2020-05-05");
          fastInputKeyCache.calcDigest(fp, spawn, context.getMetadataProvider());
          byte[] spawnInputCacheKey = fp.digestAndReset();
          Digest aliasRootDigest = digestUtil.compute(spawnInputCacheKey);
          Action aliasAction =
              RemoteSpawnRunner.buildAction(commandHash, aliasRootDigest, context.getTimeout(), spawnCacheableRemotely);
          aliasActionKey = digestUtil.computeActionKey(aliasAction);
          parseTime.stop();
          spawnMetrics.setParseTime(parseTime.elapsed());
        }
      }
    }
    return aliasActionKey;
  }

  public ActionKey getCachedActionKey() throws IOException, InterruptedException {
    // Look up action cache, and reuse the action output if it is found.
    if (remoteOptions.remoteAcceptActionAliases && spawnCacheableRemotely && !cachedActionKeyCalculated) {
      synchronized (this) {
        if (!cachedActionKeyCalculated) {
          Context withMetadata =
              TracingMetadataUtils.contextWithMetadata(buildRequestId, commandId, getAliasActionKey())
                  .withValue(NetworkTime.CONTEXT_KEY, networkTime);
          Context previous = withMetadata.attach();
          try (SilentCloseable c =
              Profiler.instance().profile(ProfilerTask.REMOTE_CACHE_CHECK, "fast alias cache check")) {
            ActionResult aliasCachedResult = remoteCache.downloadActionResult(getAliasActionKey(), /* inlineOutErr= */ false);
            if (aliasCachedResult != null) {
              cachedActionKey = new ActionKey(aliasCachedResult.getStdoutDigest());
            }
          } catch (IOException e) {
          } finally {
            withMetadata.detach(previous);
          }
          cachedActionKeyCalculated = true;
        }
      }
    }
    return cachedActionKey;
  }

  public Action getAction() throws IOException {
    Digest rootDigest = getMerkleTree().getRootDigest();
    Action action =
        RemoteSpawnRunner.buildAction(
            commandHash, rootDigest, context.getTimeout(), spawnCacheableRemotely);
    return action;
  }

  public ActionKey getActionKey() throws IOException, InterruptedException {
    if (actionKey == null) {
      synchronized (this) {
        if (actionKey == null) {
          actionKey = getCachedActionKey();
          if (actionKey == null) {
            Action action = getAction();
            actionKey = digestUtil.computeActionKey(action);
          }
        }
      }
    }
    return actionKey;
  }

  public boolean uploadActionAlias() throws IOException, InterruptedException {
    return getAliasActionKey() != null && getCachedActionKey() == null && remoteOptions.remoteUploadActionAliases;
  }
}
