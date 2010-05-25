/*
 * Copyright 2009 The Apache Software Foundation Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0 Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and limitations under the
 * License.
 */

package org.apache.hadoop.hbase.regionserver.transactional;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;

import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.regionserver.KeyValueScanner;

/**
 * A fixture that implements and presents a KeyValueScanner. It takes a list of key/values which is then sorted
 * according to the provided comparator, and then the whole thing pretends to be a store file scanner.
 */
// FIXME : this code was copied over from core-hbase tests.
public class KeyValueScanFixture implements KeyValueScanner {

    ArrayList<KeyValue> data;
    Iterator<KeyValue> iter = null;
    KeyValue current = null;
    KeyValue.KVComparator comparator;

    public KeyValueScanFixture(final KeyValue.KVComparator comparator, final KeyValue ... incData) {
        this.comparator = comparator;

        data = new ArrayList<KeyValue>(incData.length);
        for (int i = 0; i < incData.length; ++i) {
            data.add(incData[i]);
        }
        Collections.sort(data, this.comparator);
        this.iter = data.iterator();
        this.current = data.size() > 0 ? data.get(0) : null;
    }

    @Override
    public KeyValue peek() {
        return this.current;
    }

    @Override
    public KeyValue next() {
        KeyValue res = current;

        if (iter.hasNext())
            current = iter.next();
        else
            current = null;
        return res;
    }

    @Override
    public boolean seek(final KeyValue key) {
        // start at beginning.
        iter = data.iterator();
        int cmp;
        KeyValue kv = null;
        do {
            if (!iter.hasNext()) {
                current = null;
                return false;
            }
            kv = iter.next();
            cmp = comparator.compare(key, kv);
        } while (cmp > 0);
        current = kv;
        return true;
    }

    @Override
    public void close() {
    // noop.
    }
}
