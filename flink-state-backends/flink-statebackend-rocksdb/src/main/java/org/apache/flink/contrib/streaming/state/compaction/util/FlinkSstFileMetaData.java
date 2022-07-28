/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.compaction.util;

import org.rocksdb.SstFileMetaData;

import java.util.Arrays;
import java.util.Objects;

public class FlinkSstFileMetaData {
    private final String fileName;
    private final String path;
    private final long size;
    private final long smallestSeqno;
    private final long largestSeqno;
    private final byte[] smallestKey;
    private final byte[] largestKey;
    private final long numReadsSampled;
    private boolean beingCompacted;
    private final long numEntries;
    private final long numDeletions;
    private boolean toCompactByKeyElimination;

    public FlinkSstFileMetaData(SstFileMetaData file) {
        this.fileName = file.fileName();
        this.path = file.path();
        this.size = file.size();
        this.smallestSeqno = file.smallestSeqno();
        this.largestSeqno = file.largestSeqno();
        this.smallestKey = file.smallestKey();
        this.largestKey = file.largestKey();
        this.numReadsSampled = file.numReadsSampled();
        this.beingCompacted = file.beingCompacted();
        this.numEntries = file.numEntries();
        this.numDeletions = file.numDeletions();
        this.toCompactByKeyElimination = false;
    }

    public FlinkSstFileMetaData(
            String fileName,
            String path,
            long size,
            long smallestSeqno,
            long largestSeqno,
            byte[] smallestKey,
            byte[] largestKey,
            long numReadsSampled,
            boolean beingCompacted,
            long numEntries,
            long numDeletions) {
        this.fileName = fileName;
        this.path = path;
        this.size = size;
        this.smallestSeqno = smallestSeqno;
        this.largestSeqno = largestSeqno;
        this.smallestKey = smallestKey;
        this.largestKey = largestKey;
        this.numReadsSampled = numReadsSampled;
        this.beingCompacted = beingCompacted;
        this.numEntries = numEntries;
        this.numDeletions = numDeletions;
        this.toCompactByKeyElimination = false;
    }

    public String fileName() {
        return fileName;
    }

    public String path() {
        return path;
    }

    public long size() {
        return size;
    }

    public long smallestSeqno() {
        return smallestSeqno;
    }

    public long largestSeqno() {
        return largestSeqno;
    }

    public byte[] smallestKey() {
        return smallestKey;
    }

    public byte[] largestKey() {
        return largestKey;
    }

    public long numReadsSampled() {
        return numReadsSampled;
    }

    public boolean beingCompacted() {
        return beingCompacted;
    }

    public long numEntries() {
        return numEntries;
    }

    public long numDeletions() {
        return numDeletions;
    }

    public void setBeingCompacted(boolean beingCompacted) {
        this.beingCompacted = beingCompacted;
    }

    public boolean isToCompactByKeyElimination() {
        return toCompactByKeyElimination;
    }

    public void setToCompactByKeyElimination(boolean toCompactByKeyElimination) {
        this.toCompactByKeyElimination = toCompactByKeyElimination;
    }

    @Override
    public String toString() {
        return String.format(
                "File{f:%s, sk:%s, lk:%s, cmp: %s, kE: %s}",
                fileName,
                Arrays.toString(new byte[] {smallestKey[0], smallestKey[1]}),
                Arrays.toString(new byte[] {largestKey[0], largestKey[1]}),
                beingCompacted ? "true" : "false",
                toCompactByKeyElimination ? "true" : "false");
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        FlinkSstFileMetaData that = (FlinkSstFileMetaData) o;

        return Objects.equals(fileName, that.fileName);
    }

    @Override
    public int hashCode() {
        return fileName != null ? fileName.hashCode() : 0;
    }
}
