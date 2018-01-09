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

Hints
-----

Hints in Cassandra provide a mechanism for ensuring consistency amongst replicas across configurable periods of downtime, and when replicas fail to acknowledge a write.

Each hint represents a single mutation which is a write made by a client. Hints are stored as flat-files on disk, and are labelled as ``<hostID>-<timestamp>-<version>.hints``. A single file contains the stored hints for exactly one node in the cluster, based off its ``hostID`` as reported in ``nodetool status``. A host can have multiple hint files, as there is a size limit on a single hintfile of 128MB, these are differentiated by the ``timestamp`` which is the point at which the hint file is created. ``<version>`` is the hint format version of the file. A crc file is created alongside each hint file, which covers all hints in the file.

By default a coordinator will store the first 3 hours of hints for a node, and this is configurable by the yaml property `max_hint_window_in_ms`_. After a node has been down for longer than the configured  window, no more hints are stored and a repair will be necessary after the node is recovered. Replayment of hints is attempted every 10 seconds, and continues until the hints are successful. If a hint fails to   be replayed the position in the hint file will be reset and it will automatically be retried at the next hint replay attempt.

Note that a node that has been down will serve inconsistent data until all hints destined for that node are replayed, and if the node has been down longer than the configured hint window data on that node will be inconsistent until a repair has occured for the affected data.

A hint is stored for all replicas on the coordinator node in the following cases:

1. When a subset of replicas are down, but enough are still alive to satisfy a consistency level >= ``ONE``.
2. When all replicas are down but the consistency level for the query was ``ANY``.
3. When a replica that was sent the mutation doesn't acknowledge it successfully applied. I.e, the query times out.

A hint does not count towards the Consistency Level for any level other than  `ANY`_. An INSERT at ANY will be successful as long as *at least* one hint is written.

.. _ANY: /doc/latest/architecture/dynamo.html?highlight=consistency%20level#tunable-consistency
.. _max_hint_window_in_ms: /doc/latest/configuration/cassandra_config_file.html#max-hint-window-in-ms
