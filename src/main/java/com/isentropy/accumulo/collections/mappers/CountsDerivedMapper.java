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

package com.isentropy.accumulo.collections.mappers;

import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;

import com.isentropy.accumulo.collections.DerivedMapper;
import com.isentropy.accumulo.collections.io.LongBinarySerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.iterators.LongCountAggregateIterator;

/**
 * derives a map of (last key seen per tablet server, count of seen entries per tablet server)
 */

public class CountsDerivedMapper implements DerivedMapper{

	@Override
	public Class<? extends SortedKeyValueIterator<Key, Value>> getIterator() {
		return LongCountAggregateIterator.class;
	}

	@Override
	public Map<String, String> getIteratorOptions() {
		return null;
	}

	@Override
	public SerDe getDerivedMapValueSerde() {
		return new LongBinarySerde();
	}

}
