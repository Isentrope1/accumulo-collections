/* 
Accumulo Collections
Copyright 2016 Isentropy LLC
Written by Jonathan Wolff <jwolff@isentropy.com>
Isentropy specializes in big data and quantitative programming consulting,
with particular expertise in Accumulo development and installation. 
More info at http://isentropy.com.


Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package com.isentropy.accumulo.collections;

import java.util.Iterator;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;

import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.IteratorSetting.Column;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.hadoop.io.Text;

/**
 * a dummy Scanner that returns no rows
 */

public class EmptyScanner implements Scanner {

	public EmptyScanner() {
	}

	@Override
	public void addScanIterator(IteratorSetting arg0) {
	}

	@Override
	public void clearColumns() {
	}

	@Override
	public void clearScanIterators() {
	}

	@Override
	public void close() {
	}

	@Override
	public void fetchColumn(Column arg0) {
	}

	@Override
	public void fetchColumn(Text arg0, Text arg1) {
	}

	@Override
	public void fetchColumnFamily(Text arg0) {
	}

	@Override
	public Authorizations getAuthorizations() {
		return null;
	}

	@Override
	public long getTimeout(TimeUnit arg0) {
		return 0;
	}

	@Override
	public Iterator<Entry<Key, Value>> iterator() {
		return new Iterator<Entry<Key, Value>>(){

			@Override
			public boolean hasNext() {
				return false;
			}

			@Override
			public Entry<Key, Value> next() {
				return null;
			}};
	}

	@Override
	public void removeScanIterator(String arg0) {
	}

	@Override
	public void setTimeout(long arg0, TimeUnit arg1) {
	}

	@Override
	public void updateScanIteratorOption(String arg0, String arg1, String arg2) {
	}

	@Override
	public void disableIsolation() {
	}

	@Override
	public void enableIsolation() {
	}

	@Override
	public int getBatchSize() {
		return 0;
	}

	@Override
	public Range getRange() {
		return null;
	}

	@Override
	public long getReadaheadThreshold() {
		return 0;
	}

	@Override
	public int getTimeOut() {
		return 0;
	}

	@Override
	public void setBatchSize(int arg0) {
	}

	@Override
	public void setRange(Range arg0) {
	}

	@Override
	public void setReadaheadThreshold(long arg0) {
	}

	@Override
	public void setTimeOut(int arg0) {
	}

}
