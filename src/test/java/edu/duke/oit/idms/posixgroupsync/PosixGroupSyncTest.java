package edu.duke.oit.idms.posixgroupsync;

import edu.duke.oit.idms.posixgroupsync.test.utils.MockDBConnection;
import edu.duke.oit.idms.posixgroupsync.test.utils.MockLdapContext;
import org.junit.Assert;
import org.junit.Test;

import javax.naming.NamingEnumeration;
import javax.naming.NamingException;
import javax.naming.directory.SearchResult;
import javax.naming.ldap.LdapContext;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;

public class PosixGroupSyncTest {

    private MockLdapContext mockLdapContext = new MockLdapContext();
    private MockDBConnection mockDBConnection = new MockDBConnection();



    ///////////////////////////////////////////////////////////////////////////////////////////
    // GidNumber tests
    ///////////////////////////////////////////////////////////////////////////////////////////
    @Test
    public void getGIDTest() throws SQLException{

        //Setting up a Mocked database connection object to return the gidNumber 1000000001 when queried
        String mySQLQuery = "select something from somewhere";
        long gidNumberExpected = 1000000001L;

        Connection mockedDBConn = mockDBConnection.getMockDBConnection(mySQLQuery,gidNumberExpected);


        //Making the result to the getGidNumber method as PosixGroupSync class would do:
        //  handing it a database connection and a query
        String gidNumberReturned = PosixGroupSync.getGidNumber(mySQLQuery, mockedDBConn);

        //Checking to see if the result was
        assertEquals(Long.toString(gidNumberExpected),gidNumberReturned);

    }

    @Test
    public void getGIDTest_NoResults() throws SQLException{

        //Setting up a Mocked database connection object to return an empty result
        // (-1  for the expected gidNumber triggers that)
        String mySQLQuery = "select something from somewhere";
        long gidNumberExpected = -1;
        Throwable expectedException = null;

        Connection mockedDBConn = mockDBConnection.getMockDBConnection(mySQLQuery, gidNumberExpected);

        //Making the result to the getGidNumber method as PosixGroupSync class would do:
        //  handing it a database connection and a query.
        //This should cause a RunTimeException, so looking for that in the catch block
        try {
            String gidNumberReturned = PosixGroupSync.getGidNumber(mySQLQuery, mockedDBConn);

            //If we get here, then the Exception was not throw
            Assert.fail("Should not get here, RuntimeException was not thrown");
        } catch (RuntimeException e){
            expectedException = e;
        }

        //Check to make sure this is RuntimeException
        assertTrue(expectedException instanceof RuntimeException);

        //Todo:  Since right now this will ALWAYS be a RTE, we should probably check something else
        assertEquals(expectedException.getMessage(), "No rows returned when asked for the next gidNumber.");

    }

    ///////////////////////////////////////////////////////////////////////////////////////////
    // LdapContext tests
    ///////////////////////////////////////////////////////////////////////////////////////////

    /**
     * This will test the method getLDAPGroupsToSync
     *
     * @throws NamingException
     */
    @Test
    public void testGetGroupFromLDAP() throws NamingException{

        String baseDN = "ou=unix,ou=systems,ou=oit,ou=duke,ou=groups,dc=duke,dc=edu";
        String filter = "(&(objectClass=groupOfNames)(!(objectClass=posixGroup)))";

        SearchResult expectedResult = mockLdapContext.getMockedSearchResult(
                "cn=groupName,"+baseDN,
                Collections.singletonMap("cn",Collections.singletonList("groupName")));

        List<SearchResult> searchResults = Collections.singletonList(expectedResult);

        LdapContext mockedConn = mockLdapContext.getMockedLdapContext(baseDN, filter, searchResults);

        List<SearchResult> testResults = PosixGroupSync.SearchLDAP(baseDN, mockedConn);

        assertEquals("Names should match", expectedResult.getNameInNamespace(), testResults.get(0).getNameInNamespace());

    }



}


