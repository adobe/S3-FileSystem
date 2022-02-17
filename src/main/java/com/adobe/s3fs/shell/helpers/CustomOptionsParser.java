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

package com.adobe.s3fs.shell.helpers;

import com.github.rvesse.airline.model.OptionMetadata;
import com.github.rvesse.airline.parser.ParseState;
import com.github.rvesse.airline.parser.options.AbstractOptionParser;
import com.github.rvesse.airline.parser.options.ListValueOptionParser;
import com.github.rvesse.airline.restrictions.None;
import org.apache.commons.collections4.iterators.PeekingIterator;

import java.util.List;

public class CustomOptionsParser<T> extends AbstractOptionParser<T> {

  private ListValueOptionParser<T> listValueOptionParser = new ListValueOptionParser<>();

  @Override
  public ParseState<T> parseOptions(
      PeekingIterator<String> tokens, ParseState<T> state, List<OptionMetadata> allowedOptions) {
    OptionMetadata option = findOption(state, allowedOptions, tokens.peek());
    if (option == null) {
      return null;
    }

    boolean isSplittable =
        option
            .getRestrictions()
            .stream()
            .anyMatch(c -> c instanceof None);
    if (isSplittable) {
      return listValueOptionParser.parseOptions(tokens, state, allowedOptions);
    }
    return null; // Go to next Option parsers
  }
}
