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
import java.nio.charset.StandardCharsets;
import java.util.Map;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.math3.stat.descriptive.SummaryStatistics;

import com.isentropy.accumulo.collections.io.JavaSerializationSerde;
import com.isentropy.accumulo.collections.io.SerDe;
import com.isentropy.accumulo.util.KeyValue;

import static com.isentropy.accumulo.collections.AccumuloSortedMap.OPT_KEY_SERDE;
import static com.isentropy.accumulo.collections.AccumuloSortedMap.OPT_VALUE_INPUT_SERDE;


/**
 * output value is JavaSerialized SummaryStatistics object
 */
public class StatsAggregateIterator extends AggregateIterator{
	
	protected SummaryStatistics stats = new SummaryStatistics();

	
	@Override
	public boolean validateOptions(Map<String,String> options) {
		if(!super.validateOptions(options))
			return false;
		return options.containsKey(OPT_VALUE_INPUT_SERDE);
	}
	
	@Override
	protected KeyValue aggregate() throws IOException{
		Key k=null;
		while(getSource().hasTop()){
			k= getSource().getTopKey();
			Value v = getSource().getTopValue();
			Object vo = value_serde.deserialize(v.get());
			if(vo instanceof Number){
				stats.addValue(((Number) vo).doubleValue()); 
			}
			getSource().next();
		}
		//return the java serialized stats in one Value
		return new KeyValue(k,new Value(JavaSerializationSerde.javaSerialize(stats)));
	}
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
