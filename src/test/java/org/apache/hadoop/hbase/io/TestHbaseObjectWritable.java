/**
 * Copyright 2007 The Apache Software Foundation
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.junit.Assert;

public class TestHbaseObjectWritable extends TestCase {

  @Override
  protected void setUp() throws Exception {
    super.setUp();
  }

  @Override
  protected void tearDown() throws Exception {
    super.tearDown();
  }

  @SuppressWarnings("boxing")
  public void testReadObjectDataInputConfiguration() throws IOException {
    Configuration conf = HBaseConfiguration.create();
    // Do primitive type
    final int COUNT = 101;
    assertTrue(doType(conf, COUNT, int.class).equals(COUNT));
    // Do array
    final byte [] testing = "testing".getBytes();
    byte [] result = (byte [])doType(conf, testing, testing.getClass());
    assertTrue(WritableComparator.compareBytes(testing, 0, testing.length,
       result, 0, result.length) == 0);
    // Do unsupported type.
    boolean exception = false;
    try {
      doType(conf, new Object(), Object.class);
    } catch (UnsupportedOperationException uoe) {
      exception = true;
    }
    assertTrue(exception);
    // Try odd types
    final byte A = 'A';
    byte [] bytes = new byte[1];
    bytes[0] = A;
    Object obj = doType(conf, bytes, byte [].class);
    assertTrue(((byte [])obj)[0] == A);
    // Do 'known' Writable type.
    obj = doType(conf, new Text(""), Text.class);
    assertTrue(obj instanceof Text);
    //List.class
    List<String> list = new ArrayList<String>();
    list.add("hello");
    list.add("world");
    list.add("universe");
    obj = doType(conf, list, List.class);
    assertTrue(obj instanceof List);
    Assert.assertArrayEquals(list.toArray(), ((List)obj).toArray() );
    //ArrayList.class
    ArrayList<String> arr = new ArrayList<String>();
    arr.add("hello");
    arr.add("world");
    arr.add("universe");
    obj = doType(conf,  arr, ArrayList.class);
    assertTrue(obj instanceof ArrayList);
    Assert.assertArrayEquals(list.toArray(), ((ArrayList)obj).toArray() );
    // Check that filters can be serialized
    obj = doType(conf, new PrefixFilter(HConstants.EMPTY_BYTE_ARRAY),
      PrefixFilter.class);
    assertTrue(obj instanceof PrefixFilter);
  }

  public void testCustomWritable() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    // test proper serialization of un-encoded custom writables
    CustomWritable custom = new CustomWritable("test phrase");
    Object obj = doType(conf, custom, CustomWritable.class);
    assertTrue(obj instanceof Writable);
    assertTrue(obj instanceof CustomWritable);
    assertEquals("test phrase", ((CustomWritable)obj).getValue());

    // test proper serialization of a custom filter
    CustomFilter filt = new CustomFilter("mykey");
    FilterList filtlist = new FilterList(FilterList.Operator.MUST_PASS_ALL);
    filtlist.addFilter(filt);
    obj = doType(conf, filtlist, FilterList.class);
    assertTrue(obj instanceof FilterList);
    assertNotNull(((FilterList)obj).getFilters());
    assertEquals(1, ((FilterList)obj).getFilters().size());
    Filter child = ((FilterList)obj).getFilters().get(0);
    assertTrue(child instanceof CustomFilter);
    assertEquals("mykey", ((CustomFilter)child).getKey());
  }
  
  public void testCustomWritableArray() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    // test proper serialization of un-encoded custom writables
    CustomWritable custom1 = new CustomWritable("test phrase");
    CustomWritable custom2 = new CustomWritable("test phrase2");
    CustomWritable[] customs = {custom1, custom2};
    Object obj = doType(conf, customs, CustomWritable[].class);
    
    assertTrue("Arrays should match " + Arrays.toString(customs) + ", "
               + Arrays.toString((Object[]) obj),
               Arrays.equals(customs, (Object[])obj));
  }
  
  public void testCustomSerializable() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    // test proper serialization of un-encoded serialized java objects
    CustomSerializable custom = new CustomSerializable("test phrase");
    Object obj = doType(conf, custom, CustomSerializable.class);
    assertTrue(obj instanceof Serializable);
    assertTrue(obj instanceof CustomSerializable);
    assertEquals("test phrase", ((CustomSerializable)obj).getValue());
  }
  
  
  public void testCustomSerializableArray() throws Exception {
    Configuration conf = HBaseConfiguration.create();

    // test proper serialization of un-encoded serialized java objects
    CustomSerializable custom1 = new CustomSerializable("test phrase");
    CustomSerializable custom2 = new CustomSerializable("test phrase2");
    CustomSerializable[] custom = {custom1, custom2};
    Object obj = doType(conf, custom, CustomSerializable[].class);
    assertTrue("Arrays should match " + Arrays.toString(custom) + ", "
                   + Arrays.toString((Object[]) obj),
               Arrays.equals(custom, (Object[]) obj));
  }

  private Object doType(final Configuration conf, final Object value,
      final Class<?> clazz)
  throws IOException {
    ByteArrayOutputStream byteStream = new ByteArrayOutputStream();
    DataOutputStream out = new DataOutputStream(byteStream);
    HbaseObjectWritable.writeObject(out, value, clazz, conf);
    out.close();
    ByteArrayInputStream bais =
      new ByteArrayInputStream(byteStream.toByteArray());
    DataInputStream dis = new DataInputStream(bais);
    Object product = HbaseObjectWritable.readObject(dis, conf);
    dis.close();
    return product;
  }
  
  public static class CustomSerializable implements Serializable {
    private static final long serialVersionUID = 1048445561865740633L;
    private String value = null;
    
    public CustomSerializable() {
    }
    
    public CustomSerializable(String value) {
      this.value = value;
    }
    
    public String getValue() {
      return value;
    }
    
    public void setValue(String value) {
      this.value = value;
    }
    
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof CustomSerializable) && ((CustomSerializable)obj).value.equals(value);
    }
    
    @Override
    public int hashCode() {
      return value.hashCode();
    }
    
    @Override
    public String toString() {
      return "<" + value + ">";
    }
    
  }

  public static class CustomWritable implements Writable {
    private String value = null;

    public CustomWritable() {
    }

    public CustomWritable(String val) {
      this.value = val;
    }

    public String getValue() { return value; }

    @Override
    public void write(DataOutput out) throws IOException {
      Text.writeString(out, this.value);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
      this.value = Text.readString(in);
    }
    
    @Override
    public boolean equals(Object obj) {
      return (obj instanceof CustomWritable) && ((CustomWritable)obj).value.equals(value);
    }
    
    @Override
    public int hashCode() {
      return value.hashCode();
    }
    
    @Override
    public String toString() {
      return "<" + value + ">";
    }
  }

  public static class CustomFilter extends FilterBase {
    private String key = null;

    public CustomFilter() {
    }

    public CustomFilter(String key) {
      this.key = key;
    }

    public String getKey() { return key; }

    public void write(DataOutput out) throws IOException {
      Text.writeString(out, this.key);
    }

    public void readFields(DataInput in) throws IOException {
      this.key = Text.readString(in);
    }
  }
}