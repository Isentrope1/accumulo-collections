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
import org.apache.accumulo.core.iterators.SortedKeyValueIterator;
import org.apache.accumulo.core.iterators.user.TransformingIterator.KVBuffer;

public abstract class DeserializedEntryTransformingIterator extends DeserializedTransformingIterator{

	/**
	 * 
	 * @param key
	 * @param value
	 * @return the output object. the output key is the input key
	 * @throws IOException
	 */
	protected abstract Object transform(Object key,Object value) throws IOException;
	

	@Override
	protected final void transformRange(SortedKeyValueIterator<Key, Value> input,
			KVBuffer output) throws IOException {
		while(input.hasTop()){
			Key k = input.getTopKey();
			byte[] kd = k.getRowData().toArray();
			Value v = input.getTopValue();
			Object vo = value_input_serde.deserialize(v.get());
			Object ko = key_serde.deserialize(kd);
			output.append(k, new Value(value_output_serde.serialize(transform(ko,vo))));
			input.next();
		}
	}
}
