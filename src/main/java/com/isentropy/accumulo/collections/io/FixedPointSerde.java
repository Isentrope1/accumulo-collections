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
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.mock.MockInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.isentropy.accumulo.collections.AccumuloSortedMap;
import com.isentropy.accumulo.collections.factory.AccumuloSortedMapFactory;
import com.isentropy.accumulo.util.Util;

import static com.isentropy.accumulo.collections.io.JavaSerializationSerde.javaDeserialize;
import static com.isentropy.accumulo.collections.io.JavaSerializationSerde.javaSerialize;
/**
 * This serde serializes primatives, strings and byte arrays in a compact format whose byte sort order preserves 
 * java sort order. Other java Objects are serialized with java serialization. 
 * 
 * byte, short, and int are all promoted to long when serializing
 * float is promoted to double when serializing.
 * 
 * 
 * Each elements is serialized with a version byte in 0th position and a type byte in 1st position, and the contents bytes
 * following. The type bytes are:
 * 
 * 'a': remaining bytes are contents of byte array (java byte[])
 * 
 * 'f': number between Long.MIN_VALUE and Long.MAX_VALUE written in fixed point format.
 *  First long of bytes (8 bytes) are unsigned value of floor(n), (from Long.MIN_VALUE). 
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
	public static final byte TYPEBYTE_FIXEDPOINT='f';	
	public static final byte TYPEBYTE_OBJECT='o';
	public static final byte TYPEBYTE_UTF8='s';
	protected static final long LONG_BIT64 = 0x8000000000000000l;
	protected static final byte VERSION0 = 0;

	protected byte[] writeNumberAsFixedPoint(Number n){
		boolean writeFraction = n instanceof Float || n instanceof Double;
		ByteBuffer bb = ByteBuffer.allocate(2+Long.BYTES + (writeFraction?Long.BYTES:0));
		double d = n.doubleValue();
		long l = writeFraction?(long) Math.floor(d):n.longValue();
		bb.put(VERSION0);
		bb.put(TYPEBYTE_FIXEDPOINT);
		if(l >= 0){
			bb.putLong(l|LONG_BIT64);
		}
		else{
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
			ByteBuffer bb = ByteBuffer.allocate(bytes.length+2);
			bb.put(VERSION0);
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
			ByteBuffer bb = ByteBuffer.allocate(ob.length+2);
			bb.put(VERSION0);
			bb.put(TYPEBYTE_BYTEARRAY);
			bb.put(ob);
			bb.flip();
			return bb.array();
		}

		byte[] ob = javaSerialize(o);
		ByteBuffer bb = ByteBuffer.allocate(ob.length+2);
		bb.put(VERSION0);
		bb.put(TYPEBYTE_OBJECT);
		bb.put(ob);
		bb.flip();
		return bb.array();
	}
	@Override
	public Object deserialize(byte[] b){
		ByteBuffer bb = ByteBuffer.wrap(b);
		byte version = bb.get();
		byte type = bb.get();
		switch(type){
		case TYPEBYTE_BYTEARRAY: return Arrays.copyOfRange(b, 2, b.length);
		case TYPEBYTE_FIXEDPOINT: 
			long intpart = bb.getLong();
			if((intpart&LONG_BIT64) != 0){
				//positive
				intpart=intpart^LONG_BIT64;
			}
			else{
				//negative
				 intpart = intpart + Long.MIN_VALUE;
			}
			if(!bb.hasRemaining()){
				return intpart;
			}

			double fracpart = bb.getLong();
			fracpart = fracpart/Long.MAX_VALUE;
			return intpart+fracpart;

		case TYPEBYTE_OBJECT:
			return javaDeserialize(b,2,b.length-2);
		case TYPEBYTE_UTF8:
			return new String(b,2,b.length-2,StandardCharsets.UTF_8);
		default: break;
		}
		return null;
	}
	/*
	public static void main(String[] args) throws AccumuloException, AccumuloSecurityException, InstantiationException, IllegalAccessException, ClassNotFoundException{
		Connector c = new MockInstance().getConnector("root", new PasswordToken());
		AccumuloSortedMap asm = new AccumuloSortedMapFactory(c,"factory_name").makeMap("mapname");
		asm.setClearable(true).clear();
		asm.put(Long.MIN_VALUE, "min");
		asm.put(-100, "val");
		asm.put(-50.5, new byte[1]);
		asm.put(-0.1, "n");
		asm.put(0, "x".toCharArray());
		asm.put(0.1, "p");
		asm.put(50.5, "");
		asm.put(100, "");
		asm.put(Long.MAX_VALUE, "maxx");	
		asm.dump(System.out);
	}
	*/
	
}
