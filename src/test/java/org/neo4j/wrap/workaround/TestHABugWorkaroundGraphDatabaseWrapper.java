/**
 * Copyright (c) 2002-2011 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.wrap.workaround;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashSet;
import java.util.Set;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.PropertyContainer;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.EmbeddedGraphDatabase;
import org.neo4j.test.TargetDirectory;

import scala.actors.threadpool.Arrays;

public class TestHABugWorkaroundGraphDatabaseWrapper
{
    private static final TargetDirectory target = TargetDirectory
            .forTest( TestHABugWorkaroundGraphDatabaseWrapper.class );
    private static GraphDatabaseService graphdb;

    @BeforeClass
    public static void startGraphDB() throws Exception
    {
        graphdb = new HABugWorkaroundGraphDatabaseWrapper( new EmbeddedGraphDatabase( target.graphDbDir( true )
                .getAbsolutePath() ) );
    }

    @AfterClass
    public static void stopGraphDB() throws Exception
    {
        if ( graphdb != null ) graphdb.shutdown();
        graphdb = null;
    }

    enum TestTypes implements RelationshipType
    {
        TEST
    }

    enum EntityType
    {
        NODE
        {
            @Override
            PropertyContainer create( TestHABugWorkaroundGraphDatabaseWrapper test )
            {
                return test.createNode();
            }
        },
        REL
        {
            @Override
            PropertyContainer create( TestHABugWorkaroundGraphDatabaseWrapper test )
            {
                return test.createRelationship();
            }
        };
        abstract PropertyContainer create( TestHABugWorkaroundGraphDatabaseWrapper test );
    }

    @Test
    public void canCreateNode()
    {
        createNode();
    }

    public Node createNode()
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Node node = graphdb.createNode();
            assertNotNull( node );
            tx.success();
            return node;
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void canCreateRelationship()
    {
        createRelationship();
    }

    public Relationship createRelationship()
    {
        Transaction tx = graphdb.beginTx();
        try
        {
            Relationship rel = createNode().createRelationshipTo( createNode(), TestTypes.TEST );
            assertNotNull( rel );
            tx.success();
            return rel;
        }
        finally
        {
            tx.finish();
        }
    }

    @Test
    public void canOperateWithNodeProperty() throws Exception
    {
        canOperateWithProperty( EntityType.NODE );
    }

    @Test
    public void canOperateWithRelationshipProperty() throws Exception
    {
        canOperateWithProperty( EntityType.REL );
    }

    private void canOperateWithProperty( EntityType entityType ) throws Exception
    {
        PropertyContainer entity;
        Transaction tx = graphdb.beginTx();
        try
        {
            entity = entityType.create( this );
            assertFalse( entity.hasProperty( "key" ) );
            assertContainsAll( entity.getPropertyKeys() );
            assertContainsAll( entity.getPropertyValues() );
            entity.setProperty( "key", "value" );
            assertTrue( entity.hasProperty( "key" ) );
            assertEquals( "value", entity.getProperty( "key" ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        assertTrue( entity.hasProperty( "key" ) );
        assertEquals( "value", entity.getProperty( "key" ) );
        assertContainsAll( entity.getPropertyKeys(), "key" );
        assertContainsAll( entity.getPropertyValues(), "value" );
        tx = graphdb.beginTx();
        try
        {
            assertEquals( "value", entity.removeProperty( "key" ) );
            assertFalse( entity.hasProperty( "key" ) );

            tx.success();
        }
        finally
        {
            tx.finish();
        }
        assertFalse( entity.hasProperty( "key" ) );
        assertContainsAll( entity.getPropertyKeys() );
        assertContainsAll( entity.getPropertyValues() );
    }

    private <T> void assertContainsAll( Iterable<T> actual, T... values )
    {
        Set<T> expected = new HashSet<T>( Arrays.asList( values ) );
        for ( T value : actual )
        {
            assertTrue( String.format( "Unexpected value <%s>", value ), expected.remove( value ) );
        }
        assertTrue( String.format( "Missing values %s", expected ), expected.isEmpty() );
    }
}
