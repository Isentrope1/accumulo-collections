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
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.util.KeyValue;

public class RowStatsTransformingIterator extends DeserializedTransformingIterator{

	
	public RowStatsTransformingIterator(){
		value_output_serde = new JavaSerializationSerde();
	}
	@Override
	protected void transformRange(SortedKeyValueIterator<Key, Value> input,
			KVBuffer output) throws IOException {
		SummaryStatistics stats = new SummaryStatistics();
		Key k=null;
		while(input.hasTop()){
			k= input.getTopKey();
			Value v = input.getTopValue();
			Object vo = value_input_serde.deserialize(v.get());
			if(vo instanceof Number){
				stats.addValue(((Number) vo).doubleValue()); 
			}
			input.next();
		}
		output.append(k, new Value(value_output_serde.serialize(stats)));
	}

}
