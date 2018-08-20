/*
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
package org.apache.cassandra.auth;

import java.util.Set;

import com.google.common.base.Objects;

import org.apache.cassandra.config.DatabaseDescriptor;
import org.apache.cassandra.dht.Datacenters;
import org.apache.cassandra.utils.Pair;

/**
 * Returned from IAuthenticator#authenticate(), represents an authenticated user everywhere internally.
 *
 * Holds the name of the user and the roles that have been granted to the user. The roles will be cached
 * for roles_validity_in_ms.
 */
public class AuthenticatedUser
{
    public static final String SYSTEM_USERNAME = "system";
    public static final AuthenticatedUser SYSTEM_USER = new AuthenticatedUser(SYSTEM_USERNAME);

    public static final String ANONYMOUS_USERNAME = "anonymous";
    public static final AuthenticatedUser ANONYMOUS_USER = new AuthenticatedUser(ANONYMOUS_USERNAME);
    // User-level permissions cache.
    private static final AuthCache<Pair<AuthenticatedUser, IResource>, Set<Permission>> permissionsCache = new AuthCache.Builder<Pair<AuthenticatedUser, IResource>, Set<Permission>>()
                                                                                                           .withLoadFunction((p) -> DatabaseDescriptor.getAuthorizer().authorize(p.left, p.right))
                                                                                                           .withCacheEnabled(() -> DatabaseDescriptor.getAuthorizer().requireAuthorization())
                                                                                                           .build("PermissionsCache");

    private static final AuthCache<RoleResource, DCPermissions> networkAuthCache = new AuthCache.Builder<RoleResource, DCPermissions>()
                                                                                   .withLoadFunction(DatabaseDescriptor.getNetworkAuthorizer()::authorize)
                                                                                   .withCacheEnabled(() -> DatabaseDescriptor.getAuthenticator().requireAuthentication())
                                                                                   .build("NetworkAuthCache");

    private final String name;
    // primary Role of the logged in user
    private final RoleResource role;

    public AuthenticatedUser(String name)
    {
        this.name = name;
        this.role = RoleResource.role(name);
    }

    public String getName()
    {
        return name;
    }

    public RoleResource getPrimaryRole()
    {
        return role;
    }

    /**
     * Checks the user's superuser status.
     * Only a superuser is allowed to perform CREATE USER and DROP USER queries.
     * Im most cased, though not necessarily, a superuser will have Permission.ALL on every resource
     * (depends on IAuthorizer implementation).
     */
    public boolean isSuper()
    {
        return !isAnonymous() && Roles.hasSuperuserStatus(role);
    }

    /**
     * If IAuthenticator doesn't require authentication, this method may return true.
     */
    public boolean isAnonymous()
    {
        return this == ANONYMOUS_USER;
    }

    /**
     * Some internal operations are performed on behalf of Cassandra itself, in those cases
     * the system user should be used where an identity is required
     * see CreateRoleStatement#execute() and overrides of AlterSchemaStatement#createdResources()
     */
    public boolean isSystem()
    {
        return this == SYSTEM_USER;
    }

    /**
     * Get the roles that have been granted to the user via the IRoleManager
     *
     * @return a list of roles that have been granted to the user
     */
    public Set<RoleResource> getRoles()
    {
        return Roles.getRoles(role);
    }

    public Set<Permission> getPermissions(IResource resource)
    {
        return permissionsCache.get(Pair.create(this, resource));
    }

    public boolean hasLocalAccess()
    {
        return networkAuthCache.get(this.getPrimaryRole()).canAccess(Datacenters.thisDatacenter());
    }

    @Override
    public String toString()
    {
        return String.format("#<User %s>", name);
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o)
            return true;

        if (!(o instanceof AuthenticatedUser))
            return false;

        AuthenticatedUser u = (AuthenticatedUser) o;

        return Objects.equal(name, u.name);
    }

    @Override
    public int hashCode()
    {
        return Objects.hashCode(name);
    }
}
