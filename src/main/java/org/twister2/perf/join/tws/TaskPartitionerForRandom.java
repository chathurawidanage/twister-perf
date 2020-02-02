//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//  http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.
package org.twister2.perf.join.tws;

import edu.iu.dsc.tws.api.compute.TaskPartitioner;

import java.util.Arrays;
import java.util.Set;

public class TaskPartitionerForRandom implements TaskPartitioner<byte[]> {

  protected int keysToOneTask;
  protected int[] destinationsList;

  public TaskPartitionerForRandom() {
  }

  @Override
  public void prepare(Set<Integer> sources, Set<Integer> destinations) {
    int totalPossibilities = 256 * 256; //considering only most significant bytes of array
    this.keysToOneTask = (int) Math.ceil(totalPossibilities / (double) destinations.size());
    this.destinationsList = new int[destinations.size()];
    int index = 0;
    for (int i : destinations) {
      destinationsList[index++] = i;
    }
    Arrays.sort(this.destinationsList);
  }

  protected int getIndex(byte[] array) {
    int key = ((array[0] & 0xff) << 8) + (array[1] & 0xff);
    return key / keysToOneTask;
  }

  @Override
  public int partition(int source, byte[] data) {
    return this.destinationsList[this.getIndex(data)];
  }

  @Override
  public void commit(int source, int partition) {

  }
}
