.. Licensed to the Apache Software Foundation (ASF) under one
.. or more contributor license agreements.  See the NOTICE file
.. distributed with this work for additional information
.. regarding copyright ownership.  The ASF licenses this file
.. to you under the Apache License, Version 2.0 (the
.. "License"); you may not use this file except in compliance
.. with the License.  You may obtain a copy of the License at
..
..     http://www.apache.org/licenses/LICENSE-2.0
..
.. Unless required by applicable law or agreed to in writing, software
.. distributed under the License is distributed on an "AS IS" BASIS,
.. WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
.. See the License for the specific language governing permissions and
.. limitations under the License.

.. highlight:: none

Read repair
-----------

Read repair occurs when an inconsistency is found amongst the queried replicas during a read. The purpose of read repair is to actively repair inconsistent data as it is found. Read repair only affects rows and cells that are read, so it **should not** be relied upon to "repair" your cluster completely. Manually run repairs are still necessary to ensure consistent data and avoid zombie data.

To perform a read we issue a data query to the fastest replica, and a digest query to all the others. Based on the consistency level chosen, we wait for the desired number of acknowledgements from the replicas  and once we receive enough acknowledgements we compare the digests and data to ensure consistency. If there is a mismatch between digests we then perform a read repair, which will update the involved replicas with the latest copy of the data, ensuring both have the same data. With a consistency level less than ``ALL`` the queried data from the fastest replica will be returned immediately, and the read repair will occur in the background. This means that in almost all cases, at most the first instance of a query will return old data.

**Note:** Only at a consistency level of ``ALL`` will **all** replicas be repaired.

Range scans are not per-key and do not perform read repair. A range scan at CL > ONE *will* reconcile differences in replicas required to achieve the given CL, but extra replicas are not compared in the background and will not be repaired.
