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

import org.apache.flink.util.Preconditions;

import java.util.Comparator;

public class DefaultComparator implements Comparator<InternalKey> {
    private DefaultComparator() {}

    private static DefaultComparator instance;

    public static DefaultComparator getInstance() {
        if (instance == null) {
            synchronized (DefaultComparator.class) {
                if (instance == null) {
                    instance = new DefaultComparator();
                }
            }
        }
        return instance;
    }

    @Override
    public int compare(InternalKey akey, InternalKey bkey) {
        Preconditions.checkArgument(akey != null & bkey != null);
        byte[] abytes = akey.getRep();
        byte[] bbytes = bkey.getRep();
        int minLen = Math.min(abytes.length, bbytes.length);

        for (int i = 0; i < minLen; i++) {
            if (abytes[i] != bbytes[i]) {
                return Byte.toUnsignedInt(abytes[i]) - Byte.toUnsignedInt(bbytes[i]) > 0 ? 1 : -1;
            }
        }

        if (abytes.length < bbytes.length) {
            return -1;
        } else if (abytes.length > bbytes.length) {
            return 1;
        }

        long anum = akey.getSeqno();
        long bnum = bkey.getSeqno();
        if (anum > bnum) {
            return -1;
        } else if (anum < bnum) {
            return 1;
        }

        return 0;
    }
}
