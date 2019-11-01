﻿/*
 * Copyright 2019 GridGain Systems, Inc. and Contributors.
 * 
 * Licensed under the GridGain Community Edition License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.gridgain.com/products/software/community-edition/gridgain-community-edition-license
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

namespace Apache.Ignite.Core.Tests.Client.Cluster
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Apache.Ignite.Core.Client;
    using Apache.Ignite.Core.Cluster;
    using Apache.Ignite.Core.Impl.Client.Cluster;
    using NUnit.Framework;

    /// <summary>
    /// Cluster group API tests for thin client.
    /// </summary>
    [TestFixture]
    public class ClientClusterGroupTests : ClientTestBase
    {
        /// <summary>
        /// Test thin client cluster group returns the same nodes collection as thick one.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodesAsThickOne()
        {
            var nodes = Ignition.GetIgnite().GetCluster().GetNodes();
            var clientNodes = Client.GetCluster().GetNodes();

            Assert.IsNotEmpty(nodes);
            Assert.IsNotEmpty(clientNodes);
            AssertExtensions.ReflectionEqual(nodes, clientNodes);
        }

        /// <summary>
        /// Test thin client cluster group returns the same node as thick one.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodeAsThickOne()
        {
            var node = Ignition.GetIgnite().GetCluster().GetNode();
            var clientNode = Client.GetCluster().GetNode();

            AssertExtensions.ReflectionEqual(node, clientNode);
        }

        /// <summary>
        /// Test cluster returns the same node instance over
        /// the different calls when no topology changes have been made.
        /// </summary>
        [Test]
        public void TestClusterGroupsReturnsTheSameNodeWithSameTopology()
        {
            var clientNode = Client.GetCluster().GetNodes().SingleOrDefault();
            var clientNode2 = Client.GetCluster().GetNodes().SingleOrDefault();

            Assert.AreSame(clientNode, clientNode2);
            Assert.AreSame(Client.GetCluster().GetNode(), Client.GetCluster().GetNode());
        }

        /// <summary>
        /// Test cluster returns node by id.
        /// </summary>
        [Test]
        public void TestClusterGroupReturnsNodeById()
        {
            var node = Ignition.GetIgnite().GetCluster().GetNode();

            var clientNode = Client.GetCluster().GetNode(node.Id);

            AssertExtensions.ReflectionEqual(node, clientNode);
        }

        /// <summary>
        /// Test cluster throws exception when node is accessed with empty Guid.
        /// </summary>
        [Test]
        public void TestClusterGroupGetNodeChecksNodeId()
        {
            TestDelegate action = () => Client.GetCluster().GetNode(Guid.Empty);

            var ex = Assert.Throws<ArgumentException>(action);
            Assert.AreEqual("Node id should not be empty.", ex.Message);
        }

        /// <summary>
        /// Test cluster group reflects new nodes changes.
        /// </summary>
        [Test]
        public void TestClusterGroupDetectsNewTopologyChanges()
        {
            var nodes = Ignition.GetIgnite().GetCluster().GetNodes();
            var clientNodes = Client.GetCluster().GetNodes();

            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            using (Ignition.Start(cfg))
            {
                var nodesNew = Ignition.GetIgnite().GetCluster().GetNodes();
                var clientNodesNew = Client.GetCluster().GetNodes();

                Assert.AreEqual(2, clientNodesNew.Count);
                AssertExtensions.ReflectionEqual(nodesNew, clientNodesNew);

                var newNode = nodesNew.Single(x => x.Id == nodes.Single().Id);
                var newClientNode = clientNodesNew.Single(x => x.Id == clientNodes.Single().Id);

                AssertExtensions.ReflectionEqual(newNode, newClientNode);
            }
        }

        /// <summary>
        /// Test cluster group throws exception if unknown
        /// node ids have been requested from a client.
        /// </summary>
        [Test]
        public void TestClusterGroupThrowsExceptionInCaseOfUnknownNodes()
        {
            var invalidNodeIds = new List<Guid> {Guid.Empty};
            var clusterGroup = (ClientClusterGroup) Client.GetCluster();

            var cfg = GetIgniteConfiguration();
            cfg.AutoGenerateIgniteInstanceName = true;

            clusterGroup.UpdateTopology(1000L, invalidNodeIds);

            TestDelegate action = () => clusterGroup.GetNode();

            Assert.Throws<KeyNotFoundException>(action);
        }

        /// <summary>
        /// Test cluster group doesn't update properties if no changes have been detected.
        /// </summary>
        [Test]
        public void TestClusterGroupDoNotUpdateTopologyIfNoChangesDetected()
        {
            var clusterGroup = (ClientClusterGroup) Client.GetCluster();
            IClusterNode node = clusterGroup.GetNode();

            // Set the wrong ids, but keep the same topology version.
            var invalidNodeIds = new List<Guid> {Guid.NewGuid(), Guid.Empty};
            clusterGroup.UpdateTopology(1L, invalidNodeIds);

            Assert.AreSame(node, clusterGroup.GetNode());
        }

        /// <summary>
        /// Test cluster group applies a native predicate to nodes result set.
        /// </summary>
        [Test]
        public void TestClusterGroupAppliesPredicate()
        {
            var node = Ignition.GetIgnite().GetCluster().GetNode();

            var clientNode = Client
                .GetCluster()
                .ForPredicate(x => x.Id != Guid.Empty)
                .ForPredicate(x => x.Id != node.Id)
                .GetNode();

            Assert.IsNull(clientNode);
        }
    }
}
