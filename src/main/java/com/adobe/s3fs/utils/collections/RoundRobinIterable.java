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

package com.adobe.s3fs.utils.collections;

import com.google.common.collect.FluentIterable;
import com.google.common.collect.Iterators;
import com.google.common.collect.Lists;

import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;

public class RoundRobinIterable<T> implements Iterable<T> {

  private final Collection<? extends Iterable<T>> sourceIterables;

  public RoundRobinIterable(Collection<? extends Iterable<T>> sourceIterables) {
    this.sourceIterables = sourceIterables;
  }

  @Override
  public Iterator<T> iterator() {
    List<Iterator<T>> iterators = Lists.newLinkedList(FluentIterable.from(sourceIterables)
                                                          .transform(Iterable::iterator)
                                                          .filter(Iterator::hasNext));
    return new RoundRobinIterator<>(iterators);
  }

  private static class RoundRobinIterator<T> implements Iterator<T> {

    private final Iterator<Iterator<T>> iterators;
    private Optional<T> currentIterator;

    public RoundRobinIterator(Collection<Iterator<T>> iterators) {
      this.iterators = Iterators.cycle(iterators);
      this.currentIterator = moveNext();
    }

    @Override
    public boolean hasNext() {
      return currentIterator.isPresent();
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }

      T returnValue = currentIterator.get();
      currentIterator = moveNext();
      return returnValue;
    }

    private Optional<T> moveNext() {
      if (!iterators.hasNext()) {
        return Optional.empty();
      }

      Iterator<T> nextIterator = iterators.next();
      T currentElement = nextIterator.next();
      if (!nextIterator.hasNext()) {
        iterators.remove();
      }
      return Optional.of(currentElement);
    }

    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }
  }
}
