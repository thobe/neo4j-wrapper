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

import java.util.Map;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.kernel.AbstractGraphDatabase;
import org.neo4j.kernel.HighlyAvailableGraphDatabase;
import org.neo4j.wrap.WrappedGraphDatabase;
import org.neo4j.wrap.WrappedNode;
import org.neo4j.wrap.WrappedRelationship;

public class HABugWorkaroundGraphDatabaseWrapper extends WrappedGraphDatabase
{
    public HABugWorkaroundGraphDatabaseWrapper( String storeDir, Map<String, String> config )
    {
        this( new HighlyAvailableGraphDatabase( storeDir, config ) );
    }

    public HABugWorkaroundGraphDatabaseWrapper( AbstractGraphDatabase graphdb )
    {
        super( graphdb );
    }

    @Override
    protected WrappedNode<? extends WrappedGraphDatabase> node( Node node, boolean created )
    {
        return new LookupNode( this, node.getId() );
    }

    @Override
    protected WrappedRelationship<? extends WrappedGraphDatabase> relationship( Relationship relationship,
            boolean created )
    {
        return new LookupRelationship( this, relationship.getId() );
    }

    private static class LookupNode extends WrappedNode<HABugWorkaroundGraphDatabaseWrapper>
    {
        private final long id;

        LookupNode( HABugWorkaroundGraphDatabaseWrapper graphdb, long id )
        {
            super( graphdb );
            this.id = id;
        }

        @Override
        protected Node actual()
        {
            return graphdb.graphdb.getNodeById( id );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public int hashCode()
        {
            return (int) ( ( id >>> 32 ) ^ id );
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj ) return true;
            if ( obj instanceof LookupNode )
            {
                LookupNode that = (LookupNode) obj;
                return this.id == that.id && this.graphdb == that.graphdb;
            }
            return false;
        }
    }

    private static class LookupRelationship extends WrappedRelationship<HABugWorkaroundGraphDatabaseWrapper>
    {
        private final long id;

        LookupRelationship( HABugWorkaroundGraphDatabaseWrapper graphdb, long id )
        {
            super( graphdb );
            this.id = id;
        }

        @Override
        protected Relationship actual()
        {
            return graphdb.graphdb.getRelationshipById( id );
        }

        @Override
        public long getId()
        {
            return id;
        }

        @Override
        public int hashCode()
        {
            return (int) ( ( id >>> 32 ) ^ id );
        }

        @Override
        public boolean equals( Object obj )
        {
            if ( this == obj ) return true;
            if ( obj instanceof LookupRelationship )
            {
                LookupRelationship that = (LookupRelationship) obj;
                return this.id == that.id && this.graphdb == that.graphdb;
            }
            return false;
        }
    }
}
