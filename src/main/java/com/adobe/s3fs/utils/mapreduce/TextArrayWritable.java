/*
Copyright 2021 Adobe. All rights reserved.
This file is licensed to you under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License. You may obtain a copy
of the License at http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software distributed under
the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR REPRESENTATIONS
OF ANY KIND, either express or implied. See the License for the specific language
governing permissions and limitations under the License.
*/

package com.adobe.s3fs.utils.mapreduce;

import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TextArrayWritable extends ArrayWritable {

  public TextArrayWritable() {
    super(Text.class);
  }

  public TextArrayWritable(Text[] textArrayWritable) {
    super(Text.class, textArrayWritable);
  }

  public List<String> toStringList() {
    return Arrays.stream(get())
        .map(Writable::toString) // Text overrides toString()
        .collect(Collectors.toList());
  }

  @Override
  public String toString() {
    return Arrays.toString(toStrings());
  }

  public static TextArrayWritable fromList(List<String> strings) {
    Text[] textArr = strings.stream()
        .map(Text::new)
        .toArray(Text[]::new);
    return new TextArrayWritable(textArr);
  }
}
