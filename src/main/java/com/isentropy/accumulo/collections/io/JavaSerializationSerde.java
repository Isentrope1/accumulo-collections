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

package com.isentropy.accumulo.collections.io;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.util.Util;

public class JavaSerializationSerde implements SerDe{
	public static Logger log = LoggerFactory.getLogger(JavaSerializationSerde.class);

	public static byte[] javaSerialize(Object o){
		ByteArrayOutputStream bos = new ByteArrayOutputStream() ;
		ObjectOutputStream out;
		try {
			out = new ObjectOutputStream(bos);
			out.writeObject(o);
			out.close();
		} catch (IOException e) {
			log.error(e.getMessage());
			return null;
		}
		byte[] rslt= bos.toByteArray();
		//log.debug("serialize: "+o+" = "+rslt.length+" "+Util.bytesToHex(rslt));
		return rslt;
	}
	public static Object javaDeserialize(byte[] b){
		ByteArrayInputStream bis = new ByteArrayInputStream(b);
		try{
			ObjectInputStream ois = new ObjectInputStream(bis);
			return ois.readObject();
		}
		catch(Exception e){
			log.error(e.getMessage());
			return null;
		}
	}
	
	@Override
	public byte[] serialize(Object o){	
		return javaSerialize(o);
	}
	@Override
	public Object deserialize(byte[] b){
		return javaDeserialize(b);
	}

}
