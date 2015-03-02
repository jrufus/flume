/**
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
package org.apache.flume.serialization;

import com.google.common.base.Charsets;

import java.io.*;
import java.nio.charset.Charset;

/*
 * All test cases that test FileInputStream, also apply to ResettableGenericInputStream
 */
public class TestResettableGenericInputStream extends TestResettableFileInputStream{
  @Override
  public ResettableInputStream getResettableInputStream(File file, PositionTracker tracker)
          throws IOException {
    return new ResettableGenericInputStream(new FileStreamCreator(file), tracker, (int)file.length());
  }

  @Override
  public ResettableInputStream getResettableInputStream(File file, PositionTracker tracker,
          int bufSize, Charset charset, DecodeErrorPolicy policy)
          throws IOException {
    return new ResettableGenericInputStream(new FileStreamCreator(file), tracker,
            2048, Charsets.UTF_8, policy, (int)file.length());
  }

}
