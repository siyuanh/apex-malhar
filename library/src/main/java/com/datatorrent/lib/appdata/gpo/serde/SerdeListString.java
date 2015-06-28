/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.lib.appdata.gpo.serde;

import com.datatorrent.lib.appdata.gpo.GPOByteArrayList;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class SerdeListString implements Serde
{
  public static final SerdeListString INSTANCE = new SerdeListString();
  private final GPOByteArrayList bytes = new GPOByteArrayList();

  private SerdeListString()
  {
  }

  @Override
  public Object deserializeObject(byte[] object, MutableInt offset)
  {
    int length = GPOUtils.deserializeInt(object, offset);
    int startIndex = offset.intValue();

    List<String> strings = Lists.newArrayList();
    while(startIndex + length > offset.intValue()) {
      String value = GPOUtils.deserializeString(object, offset);
      strings.add(value);
    }

    return strings;
  }

  @Override
  public byte[] serializeObject(Object object)
  {
    @SuppressWarnings("unchecked")
    List<String> strings = (List<String>) object;

    for(String string: strings) {
      byte[] stringBytes = string.getBytes();
      byte[] lengthBytes = GPOUtils.serializeInt(stringBytes.length);

      bytes.add(lengthBytes);
      bytes.add(stringBytes);
    }

    byte[] byteArray = bytes.toByteArray();
    bytes.clear();
    bytes.add(GPOUtils.serializeInt(byteArray.length));
    bytes.add(byteArray);
    byteArray = bytes.toByteArray();
    bytes.clear();
    return byteArray;
  }

  private static final Logger LOG = LoggerFactory.getLogger(SerdeListString.class);
}
