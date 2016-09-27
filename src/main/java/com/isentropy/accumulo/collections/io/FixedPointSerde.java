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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.util.Util;

import static com.isentropy.accumulo.collections.io.JavaSerializationSerde.javaDeserialize;
import static com.isentropy.accumulo.collections.io.JavaSerializationSerde.javaSerialize;
/**
 * This serde serializes primatives, strings and byte arrays in a compact format whose byte sort order preserves 
 * java sort order. Other java Objects are serialized with java serialization. 
 * 
 * byte, short, and int are all promoted to long when serializing
 * float is promoted to double when serializing
 * 
 * Each elements is serialized with a type byte in 0th position, and the contents bytes starting 
 * in 1st position. The type bytes are:
 * 
 * 'a': remaining bytes are contents of byte array (java byte[])
 * 
 * 'k','l': negative (k) or positive number (l) between  Long.MIN_VALUE and Long.MAX_VALUE.
 *  First long of bytes (8 bytes) are unsigned value of floor(n), (translated by Long.MIN_VALUE if negative). 
 *  If number is float or double, second long of bytes represents fraction of Long.MAX_VALUE to be added to result.
 *
 *  'o': java Object serialized with java serialization
 *  
 *  's': UTF8 string bytes 
 * 
 */
public class FixedPointSerde implements SerDe{
	public static Logger log = LoggerFactory.getLogger(FixedPointSerde.class);

	public static final byte TYPEBYTE_BYTEARRAY='a';
	/**
	 * the serde relies on the fact that TYPEBYTE_NEGATIVE_FIXEDPOINT byte value is one 
	 * less than TYPEBYTE_FIXEDPOINT. In the byte sort order, negative fixed points (type TYPEBYTE_NEGATIVE_FIXEDPOINT)
	 * come right before positive fixed points (TYPEBYTE_FIXEDPOINT)
	 */
	public static final byte TYPEBYTE_NEGATIVE_FIXEDPOINT='k';
	public static final byte TYPEBYTE_FIXEDPOINT='l';	
	public static final byte TYPEBYTE_OBJECT='o';
	public static final byte TYPEBYTE_UTF8='s';
	
	protected byte[] writeNumberAsFixedPoint(Number n){
		boolean writeFraction = n instanceof Float || n instanceof Double;
		ByteBuffer bb = ByteBuffer.allocate(1+Long.BYTES + (writeFraction?Long.BYTES:0));
		double d = n.doubleValue();
		long l = writeFraction?(long) Math.floor(d):n.longValue();
		if(l >= 0){
			bb.put(TYPEBYTE_FIXEDPOINT);
			bb.putLong(l);
		}
		else{
			bb.put(TYPEBYTE_NEGATIVE_FIXEDPOINT);
			bb.putLong(l-Long.MIN_VALUE);			
		}
		if(writeFraction){
			//l is floor(d) so  0 =< frac <= 1
			double frac = d-l;
			long fraction_representation = Math.round(Long.MAX_VALUE*(frac));
			bb.putLong(fraction_representation);
		}
		bb.flip();
		return bb.array();
	}

	@Override
	public byte[] serialize(Object o){	
		if(o instanceof String){
			byte[] bytes = ((String) o).getBytes(StandardCharsets.UTF_8);
			ByteBuffer bb = ByteBuffer.allocate(bytes.length+1);
			bb.put(TYPEBYTE_UTF8);
			bb.put(bytes);
			bb.flip();
			return bb.array();
		}
		if(o instanceof Number){
			Number n = (Number) o;
			double d = n.doubleValue();
			if(d >= Long.MIN_VALUE && d <= Long.MAX_VALUE)
				return writeNumberAsFixedPoint(n);
		}
		if(o instanceof byte[]){
			byte[] ob = (byte[]) o;
			ByteBuffer bb = ByteBuffer.allocate(ob.length+1);
			bb.put(TYPEBYTE_BYTEARRAY);
			bb.put(ob);
			bb.flip();
			return bb.array();
		}
		
		byte[] ob = javaSerialize(o);
		ByteBuffer bb = ByteBuffer.allocate(ob.length+1);
		bb.put(TYPEBYTE_OBJECT);
		bb.put(ob);
		bb.flip();
		return bb.array();
	}
	@Override
	public Object deserialize(byte[] b){
		ByteBuffer bb = ByteBuffer.wrap(b);
		byte type = bb.get();
		switch(type){
		case TYPEBYTE_BYTEARRAY: return Arrays.copyOfRange(b, 1, b.length);
		case TYPEBYTE_NEGATIVE_FIXEDPOINT:
		case TYPEBYTE_FIXEDPOINT: 
			long intpart = bb.getLong(); 
			if(!bb.hasRemaining())
				return type == TYPEBYTE_NEGATIVE_FIXEDPOINT ? intpart+Long.MIN_VALUE : intpart;
			double fracpart = bb.getLong();
			fracpart = fracpart/Long.MAX_VALUE;
			return type == TYPEBYTE_NEGATIVE_FIXEDPOINT ? (intpart+Long.MIN_VALUE)+fracpart : intpart+fracpart;
			
		case TYPEBYTE_OBJECT:
			return javaDeserialize(b,1,b.length-1);
		case TYPEBYTE_UTF8:
			return new String(b,1,b.length-1,StandardCharsets.UTF_8);
		default: break;
		}
		return null;
	}
}
