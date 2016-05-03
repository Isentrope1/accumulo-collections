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

package com.isentropy.accumulo.iterators;

import java.io.IOException;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.io.LongBinarySerde;
import com.isentropy.accumulo.util.KeyValue;

/**
 * 
 * this iterator computes key and value checksums 
 * 
 * computes 2 int checksums: the XOR of deserialized object and value hashCode()s.
 * 
 * note that because it operates on deserialized objects, it should produce a result that is 
 * independent of serialization
 * 
 * returns a long that is (key checksum xor int, value checksum xor int)
 * output is serialized in LongBinarySerde format
 *
 */
public class MapChecksumAggregateIterator extends AggregateIterator {
	
	public static Logger log = LoggerFactory.getLogger(MapChecksumAggregateIterator.class);

	
	private long keyChecksum=0;
	private int valueChecksum=0;
	

	@Override
	protected KeyValue aggregate() throws IOException {
		Key k=null;
		while(getSource().hasTop()){
			k= getSource().getTopKey();
			Value v = getSource().getTopValue();
			Object ko = key_serde.deserialize(k.getRowData().toArray());
			Object vo = value_serde.deserialize(v.get());
			keyChecksum += ko.hashCode();
			valueChecksum += vo.hashCode();
			getSource().next();
		}
		long checksums = (keyChecksum << 32)|valueChecksum;
		return new KeyValue(k,new Value(LongBinarySerde.longSerialize(checksums)));
	}
}
